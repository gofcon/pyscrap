"""Config-driven browser scraping: acts out an ``ApiMst`` row's
``behavior_json`` in a real browser, then reads whatever the page ended up
showing.

The counterpart to :mod:`app.scrapers.dynamic`, for sources that answer a
request only after something has been typed, picked and clicked. Everything
outside "how do I get the records" is deliberately identical to the HTTP
engine: the same ``ApiMst`` row shape, the same ``:KEY``/``${VAR}``
substitution, the same ``{selector: [record, ...]}`` handed to
:class:`app.scrapers.base.BaseScraper` to save. A result table cannot tell
which of the two produced a row, and nothing in the job layer knows either --
``request_type = 'BROWSER'`` is the only thing that decides (see
:func:`app.scrapers.make_scraper`).

What the behavior leaves behind is read four ways, chosen by
``response_type``:

- ``'dom'`` (the default) -- extract rows from the page with the CSS spec in
  ``response_parse_json``.
- ``'xhr'`` -- keep the JSON the page fetched for itself and extract from that
  with the very same selectors an HTTP row would use. Preferred where the site
  offers it: a JSON body survives a redesign that would break every CSS path.
- ``'binary'`` -- the click produced a download; the bytes are staged and
  described exactly like an HTTP download (``BaseScraper._stage_binary``).
- ``'session'`` -- log in and keep nothing but the cookies, saved as a
  Playwright storage state file that later jobs reuse. The one response_type
  whose row may leave ``output_tables_json`` empty.

Playwright is an optional dependency (``pip install -e ".[browser]"`` plus
``playwright install --with-deps chromium``), and this module is imported only
when a row actually asks for it -- so the API deployment, which never runs
browser jobs, does not need chromium to start.
"""

from __future__ import annotations

import atexit
import os
import threading
import time
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING, Any, Iterator

import httpx
from loguru import logger
from sqlmodel import Session

from app.auth_config import resolve_env_placeholders
from app.db.models import ApiMst
from app.scrapers.base import (BaseScraper, SessionExpired, merge_records,
                               rows_to_records)
from app.scrapers.dynamic import (_PAGE_DELAY_SEC, _PAGE_PARAM, _extract_json_records,
                                  _pace_host)

try:
    from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
    from playwright.sync_api import sync_playwright
except ImportError as exc:  # pragma: no cover - depends on how the host was installed
    raise ImportError(
        "request_type='BROWSER' needs playwright, which this install does not have. "
        'Install it with: pip install -e ".[browser]" && playwright install --with-deps chromium'
    ) from exc

if TYPE_CHECKING:
    from playwright.sync_api import Browser, BrowserContext, Frame, Page

# Headed is for watching a behavior fail on a dev machine; the instance has no
# display, so headless is the default and BROWSER_HEADLESS=0 is the override.
_HEADLESS = os.environ.get("BROWSER_HEADLESS", "1").lower() not in ("0", "false", "no")

# One timeout for every wait -- a step's own "timeout" overrides it. Generous
# next to an HTTP timeout on purpose: what is being waited for here is a screen
# finishing its own requests and re-rendering, not a socket.
_TIMEOUT_MS = int(os.environ.get("BROWSER_TIMEOUT_MS", "15000"))

# Every context is opened as a Korean-locale browser, which is what this
# project's sources are written for -- and, more to the point, what makes
# chromium send an Accept-Language header at all. Headless chromium sends none
# by default, and a server is free to answer a request with no stated language
# differently: KRX's ISIN screen answers it with a *broken* page (one result
# row instead of five, with the rest of the document nested inside that row's
# cell), which cost a day of chasing selectors that were never wrong. Any
# value fixes it -- 'en-US' works too -- so this is not a locale preference so
# much as making the browser state one. Overridable per row with an explicit
# Accept-Language in header_json.
_LOCALE = os.environ.get("BROWSER_LOCALE", "ko-KR")
_TIMEZONE = os.environ.get("BROWSER_TIMEZONE", "Asia/Seoul")

# Where a 'session' row leaves its cookies for later jobs to pick up. A file
# rather than .env (as KIS's token does) because a storage state is a JSON
# document, not a value -- but for the same reason: a cookie jar cannot cross
# a process boundary, and every systemd timer is its own process.
_STATE_DIR = Path(os.environ.get("BROWSER_STATE_DIR", "data/state"))

# Playwright's sync API binds its objects to the thread that created them, so
# unlike the shared httpx.Client in dynamic.py this cannot be one global
# instance -- the batch runs single-threaded, but FastAPI answers a request on
# whichever worker thread it likes. One browser per thread, reused across the
# jobs that thread runs (a chromium launch costs ~1s, far more than any single
# page in a cycle).
_local = threading.local()
_started: list[tuple[Any, Any]] = []
_started_lock = threading.Lock()


def get_browser() -> Browser:
    """This thread's chromium, launched on first use (see _local)."""
    browser = getattr(_local, "browser", None)
    if browser is not None and browser.is_connected():
        return browser
    playwright = sync_playwright().start()
    browser = playwright.chromium.launch(headless=_HEADLESS)
    _local.playwright = playwright
    _local.browser = browser
    with _started_lock:
        if not _started:
            atexit.register(close_browsers)
        _started.append((playwright, browser))
    logger.info("browser: launched chromium (headless={})", _HEADLESS)
    return browser


def close_browsers() -> None:
    """Shut every launched browser down. Registered with atexit, so neither
    entrypoint has to call it by hand.

    Closing an instance from a thread other than the one that made it is not
    something Playwright supports, so a failure here is expected rather than
    exceptional -- the chromium process dies with its parent either way, and
    an exception raised during interpreter shutdown would only bury whatever
    the run was actually reporting."""
    with _started_lock:
        instances, _started[:] = list(_started), []
    for playwright, browser in instances:
        for close in (browser.close, playwright.stop):
            try:
                close()
            except Exception as exc:  # noqa: BLE001 - see docstring
                logger.debug("browser: {} during shutdown ({})", type(exc).__name__, exc)
    _local.__dict__.pop("browser", None)
    _local.__dict__.pop("playwright", None)


class BrowserScraper(BaseScraper):
    """Acts out one ``ApiMst`` row's ``behavior_json`` and reads the result.

    ``api_url`` is the starting page: it is opened before the first step, so a
    behavior begins where the screen already is. Every string a step carries
    (a url, a value typed into a field, a script) goes through ``:KEY`` job
    parameter substitution and then ``${VAR}`` env resolution -- the same
    order, and the same two mechanisms, as a url or a payload on an HTTP row.
    Credentials therefore stay in ``.env`` here too; a login row types
    ``"${KRX_USER_ID}"``, never the id itself.
    """

    # ---- collection ------------------------------------------------------

    def _collect_once(self, session: Session | None = None) -> dict[str, list[dict[str, Any]]]:
        """One run of the behavior, read according to ``response_type``.

        Retrying this through a fresh login when the site logged us out is
        :meth:`app.scrapers.base.BaseScraper.collect`'s job, shared with the
        HTTP engine.

        A Playwright timeout is re-raised as :class:`SessionExpired` when this
        row knows how to log in again: waiting for something that is not there
        is exactly what a logged-out screen looks like, and the login screen
        has none of the elements a behavior waits for."""
        response_type = (self.api.response_type or "dom").lower()
        mapping = self.api.output_tables_json or {}
        if not mapping and response_type != "session":
            raise ValueError(f"{self.api.api_id}: output_tables_json is empty")
        try:
            return self._collect_page(response_type, mapping)
        except PlaywrightTimeoutError as exc:
            if self._login_api_id:
                # Playwright's message carries a multi-line call log; the
                # first line is the part worth logging.
                raise SessionExpired(
                    f"{self.api.api_id}: {str(exc).splitlines()[0]}") from exc
            raise

    def _collect_page(self, response_type: str, mapping: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
        spec = self.api.response_parse_json or {}
        pagination = self.api.pagination_json or {}
        if pagination.get("mode") == "page":
            # Seeded before the first run, not after it: ':PAGE' sits inside
            # the behavior, so page one has to be a number too (the same
            # reason fetch_all_pages seeds it before the first request).
            self.params.setdefault(pagination.get("param", _PAGE_PARAM),
                                   int(pagination.get("start", 1)))
        with self._open_page() as page:
            captured: list[Any] = []
            if response_type == "xhr":
                self._capture_json(page, captured)

            downloads = self.run_behavior(page)
            self._check_logged_out(page)

            if response_type == "session":
                self._save_state(page.context)
                return {}

            if response_type == "binary":
                records: list[dict[str, Any]] = []
                for filename, content in downloads:
                    records.extend(self._stage_binary(content, fallback_name=filename))
                if not records:
                    raise ValueError(f"{self.api.api_id}: no download was produced")
                return {selector: records for selector in mapping}

            if response_type == "xhr":
                if not captured:
                    raise ValueError(
                        f"{self.api.api_id}: no response matched "
                        f"response_parse_json['url'] = {spec.get('url')!r}"
                    )
                bodies = captured if spec.get("pick") == "all" else captured[-1:]
                per_selector: dict[str, list[dict[str, Any]]] = {}
                for body in bodies:
                    for selector in mapping:
                        per_selector.setdefault(selector, []).extend(
                            _extract_json_records(body, selector))
                return per_selector

            return self._collect_dom(page)

    # ---- the browser itself ----------------------------------------------

    @contextmanager
    def _open_page(self) -> Iterator[Page]:
        """A fresh context (cookies and all) per job, on the shared browser.

        Fresh because two jobs sharing a cookie jar is exactly the accident
        that makes one site's state leak into another's; the storage state
        loaded below is the deliberate version of the same thing."""
        options: dict[str, Any] = {"locale": _LOCALE, "timezone_id": _TIMEZONE}
        state = self._state_path()
        if state.exists():
            options["storage_state"] = str(state)
        headers = {
            k: self._resolve(v) if isinstance(v, str) else v
            for k, v in (self.api.header_json or {}).items()
        }
        if headers:
            options["extra_http_headers"] = headers

        context = get_browser().new_context(**options)
        context.set_default_timeout(_TIMEOUT_MS)
        page = context.new_page()
        try:
            yield page
        finally:
            context.close()

    def _resolve(self, text: str) -> str:
        """':KEY' job params first, then '${VAR}' env values -- the same order
        DynamicApiScraper.build_request applies to a url or a payload."""
        return resolve_env_placeholders(self._substitute_placeholders(text))

    # ---- behavior --------------------------------------------------------

    def run_behavior(self, page: Page) -> list[tuple[str, bytes]]:
        """Open ``api_url`` and act out every step of ``behavior_json``.

        Steps run in order against the page, or against the frame a ``frame``
        step selected -- ``Frame`` offers the same fill/click/wait_for_selector
        surface as ``Page``, so a site that hides its screen in an iframe
        (WebSquare does) needs no separate vocabulary.

        The step vocabulary, each as ``{"action": ..., ...}``:

        - ``goto``    ``url`` (absolute, or omitted to reopen ``api_url``),
                      optional ``wait_until``
        - ``fill``    ``selector``, ``value`` -- types into an input
        - ``select``  ``selector``, ``value`` -- picks a <select> option
        - ``check``   ``selector``, optional ``checked`` (default true)
        - ``click``   ``selector``
        - ``press``   ``key``, optional ``selector``
        - ``wait_for`` ``selector``, optional ``state``/``timeout``
        - ``wait_load`` optional ``state`` (default 'networkidle')
        - ``sleep``   ``seconds``
        - ``frame``   ``selector`` or ``name`` or ``url``, or ``reset``: true
        - ``eval``    ``script``
        - ``download`` ``selector`` -- clicks and keeps the file it produces

        Returns whatever ``download`` steps collected, as
        ``[(filename, bytes), ...]``."""
        target: Page | Frame = page
        downloads: list[tuple[str, bytes]] = []

        start_url = self._resolve(self.api.api_url)
        _pace_host(start_url)
        page.goto(start_url, wait_until="load")

        for number, step in enumerate(self.api.behavior_json or [], start=1):
            action = str(step.get("action") or "").lower()
            selector = step.get("selector")
            timeout = float(step["timeout"]) * 1000 if "timeout" in step else None
            logger.debug("{}: step {} {}", self.api.api_id, number, action)

            if action == "goto":
                url = self._resolve(step.get("url") or self.api.api_url)
                _pace_host(url)
                page.goto(url, wait_until=step.get("wait_until", "load"), timeout=timeout)
            elif action == "fill":
                target.fill(selector, self._resolve(str(step.get("value", ""))), timeout=timeout)
            elif action == "select":
                target.select_option(selector, self._resolve(str(step.get("value", ""))),
                                     timeout=timeout)
            elif action == "check":
                if step.get("checked", True):
                    target.check(selector, timeout=timeout)
                else:
                    target.uncheck(selector, timeout=timeout)
            elif action == "click":
                target.click(selector, timeout=timeout)
            elif action == "press":
                key = str(step.get("key", "Enter"))
                if selector:
                    target.press(selector, key, timeout=timeout)
                else:
                    page.keyboard.press(key)
            elif action == "wait_for":
                target.wait_for_selector(selector, state=step.get("state", "visible"),
                                         timeout=timeout)
            elif action == "wait_load":
                page.wait_for_load_state(step.get("state", "networkidle"), timeout=timeout)
            elif action == "sleep":
                time.sleep(float(step.get("seconds", 1)))
            elif action == "frame":
                target = page if step.get("reset") else self._frame(page, step)
            elif action == "eval":
                target.evaluate(self._resolve(str(step.get("script", ""))))
            elif action == "download":
                downloads.append(self._download(page, target, selector, timeout))
            else:
                raise ValueError(f"{self.api.api_id}: unknown behavior action {action!r} "
                                 f"at step {number}")

        return downloads

    @staticmethod
    def _frame(page: Page, step: dict[str, Any]) -> Frame:
        """The frame a ``frame`` step names, by css selector, name, or url."""
        if step.get("selector"):
            element = page.wait_for_selector(step["selector"])
            frame = element.content_frame() if element else None
        elif step.get("name"):
            frame = page.frame(name=step["name"])
        else:
            frame = page.frame(url=step["url"])
        if frame is None:
            raise ValueError(f"no frame matched {step!r}")
        return frame

    def _download(self, page: Page, target: Page | Frame, selector: str,
                  timeout: float | None) -> tuple[str, bytes]:
        """Click something that produces a file, and keep the file.

        Read back off Playwright's own temp path rather than saved through it,
        so the bytes reach ``BaseScraper._stage_binary`` in exactly the shape
        an httpx download would -- one staging area, one naming rule, one kind
        of metadata record, whichever engine fetched it."""
        with page.expect_download(timeout=timeout or _TIMEOUT_MS) as info:
            target.click(selector, timeout=timeout)
        download = info.value
        path = download.path()
        if path is None:
            raise ValueError(f"{self.api.api_id}: download failed "
                             f"({download.failure() or 'no reason given'})")
        return download.suggested_filename, Path(path).read_bytes()

    def _capture_json(self, page: Page, captured: list[Any]) -> None:
        """Keep every JSON reply whose url contains ``response_parse_json['url']``.

        Bodies are read inside the handler: once the page navigates away, the
        response they belong to is gone, and a browser row's whole reason to
        exist is that the interesting request happens mid-behavior."""
        pattern = (self.api.response_parse_json or {}).get("url") or ""

        def on_response(response: Any) -> None:
            if pattern and pattern not in response.url:
                return
            try:
                captured.append(response.json())
            except Exception:  # noqa: BLE001 - not every matching reply is JSON
                logger.debug("{}: {} was not JSON, skipped", self.api.api_id, response.url)

        page.on("response", on_response)

    # ---- reading the page ------------------------------------------------

    def _collect_dom(self, page: Page) -> dict[str, list[dict[str, Any]]]:
        """Extract the page, following ``pagination_json`` if it says how to
        reach the rest.

        Three modes, because a screen says "there is more" by offering
        something to do rather than by answering something:

            {"mode": "click", "selector": "a.next", "wait_for": "#grid tr"}
            {"mode": "scroll"}
            {"mode": "page", "param": "PAGE", "start": 1}

        ``page`` is the HTTP engine's own mode (see ``ApiMst.pagination_json``)
        with the same meaning -- a page number this side counts up -- and it
        exists here for the screen whose pager is not a link but a script:
        ``:PAGE`` goes into the behavior itself (typically an ``eval`` step
        calling the site's own paging function) and the whole behavior is
        acted out again for each page. Slower than clicking, but it is the
        only thing that works when the pager markup never arrives, and a
        re-run is what a person pressing a page number would cause anyway.

        Stopping is judged the same way ``fetch_all_pages`` judges it: an empty
        page, a page identical to the one before (an infinite-scroll list that
        stopped growing says exactly this), the control being gone, or
        ``max_pages``."""
        spec = self.api.pagination_json or {}
        if spec.get("mode") not in ("click", "scroll", "page"):
            return self.extract(page)

        max_pages = int(spec.get("max_pages", 20))
        all_records: dict[str, list[dict[str, Any]]] = {}
        seen: dict[str, dict[str, int]] = {}
        previous: dict[str, list[dict[str, Any]]] | None = None

        for attempt in range(1, max_pages + 1):
            per_selector = self.extract(page)
            count = sum(len(r) for r in per_selector.values())
            logger.info("{}: page {} -> {} record(s)", self.api.api_id, attempt, count)
            if count == 0:
                break
            if per_selector == previous:
                logger.info("{}: page {} repeats the previous one, stopping",
                            self.api.api_id, attempt)
                break
            merge_records(all_records, seen, per_selector)
            previous = per_selector

            if not self._advance(page, spec):
                break
            time.sleep(_PAGE_DELAY_SEC)

        return all_records

    def _advance(self, page: Page, spec: dict[str, Any]) -> bool:
        """Do whatever reaches the next page, or return False when nothing does."""
        if spec.get("mode") == "page":
            # Count the page number up and act the behavior out again -- the
            # behavior is what carries ':PAGE' to wherever the site wants it.
            param = spec.get("param", _PAGE_PARAM)
            self.params[param] = int(self.params.get(param, 1)) + 1
            self.run_behavior(page)
            return True

        if spec.get("mode") == "scroll":
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        else:
            control = page.query_selector(spec["selector"])
            if control is None or not control.is_enabled() or not control.is_visible():
                return False
            control.click()
        try:
            if spec.get("wait_for"):
                page.wait_for_selector(spec["wait_for"])
            else:
                page.wait_for_load_state("networkidle")
        except PlaywrightTimeoutError:
            # Nothing arrived after the click. Past the last page a site is as
            # likely to render an empty screen as to disable the control, so
            # this is a stop condition, not a failure -- the pages already
            # collected are what there was. A timeout anywhere *else* still
            # fails the job (and may mean a stale session, see collect).
            logger.info("{}: nothing followed the last page, stopping", self.api.api_id)
            return False
        return True

    def extract(self, page: Page) -> dict[str, list[dict[str, Any]]]:
        """Every selector in ``output_tables_json``, read off the page.

        ``response_parse_json`` holds the rules, either once for every selector
        (the usual case -- one screen, one table) or per selector::

            {"rows": "#grid tbody tr", "fields": ["isin", "name", "amount"]}

            {"holdings": {"rows": "#grid tbody tr", "fields": {...}},
             "summary":  {"rows": "#top li",       "fields": {...}}}

        A rules block is recognised by having ``rows``, so the reserved keys
        that sit alongside it (``state``, ``login``, ``logged_out``, ``url``)
        can never be mistaken for a selector's rules.

        ``fields`` is either a list of column names read positionally out of
        each row's cells -- the same rectangle-of-cells shape a delimited file
        or a spreadsheet arrives in, so it goes through the same
        :func:`app.scrapers.base.rows_to_records` -- or a dict naming a css
        path per field::

            {"isin": "td:nth-child(1)",
             "url":  {"css": "td:nth-child(2) a", "attr": "href"}}
        """
        spec = self.api.response_parse_json or {}
        out: dict[str, list[dict[str, Any]]] = {}
        for selector in (self.api.output_tables_json or {}):
            rules = spec.get(selector)
            if not (isinstance(rules, dict) and "rows" in rules):
                rules = spec
            if "rows" not in rules:
                raise ValueError(f"{self.api.api_id}: response_parse_json needs 'rows' "
                                 f"(a css selector for one record) for '{selector}'")
            out[selector] = self._extract_rows(page, rules)
        return out

    @staticmethod
    def _extract_rows(page: Page, rules: dict[str, Any]) -> list[dict[str, Any]]:
        elements = page.query_selector_all(rules["rows"])
        fields = rules.get("fields")

        if isinstance(fields, dict):
            records = []
            for element in elements:
                record: dict[str, Any] = {}
                for name, rule in fields.items():
                    css = rule if isinstance(rule, str) else rule.get("css")
                    attr = None if isinstance(rule, str) else rule.get("attr")
                    cell = element.query_selector(css) if css else element
                    if cell is None:
                        record[name] = None
                        continue
                    value = cell.get_attribute(attr) if attr else cell.inner_text()
                    record[name] = (value or "").strip() or None
                if any(v is not None for v in record.values()):
                    records.append(record)
            return records

        # Positional: each matched element is a row of cells. Shares its
        # skip_rows/has_header/fields handling with every other tabular source.
        cell_selector = rules.get("cells", "td, th")
        rows = [[(cell.inner_text() or "").strip()
                 for cell in element.query_selector_all(cell_selector)]
                for element in elements]
        return rows_to_records([row for row in rows if any(row)], rules)

    # ---- login and session state -----------------------------------------

    def _state_key(self) -> str:
        """Which stored session this row uses. Defaults to the site's host, so
        a login row and the rows that depend on it line up without either
        naming the other; ``response_parse_json['state']`` overrides it for a
        site whose login lives on a different host than its screens."""
        spec = self.api.response_parse_json or {}
        if spec.get("state"):
            return str(spec["state"])
        # httpx only to parse the url -- the same host-matching this project
        # already does for rate pacing (dynamic._interval_for).
        return httpx.URL(self.api.api_url).host or self.api.api_id.lower()

    def _state_path(self) -> Path:
        return _STATE_DIR / f"{self._state_key()}.json"

    def _save_state(self, context: BrowserContext) -> None:
        """response_type='session': keep the cookies, drop everything else."""
        path = self._state_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        context.storage_state(path=str(path))
        logger.info("{}: saved browser session to {}", self.api.api_id, path)

    def _check_logged_out(self, page: Page) -> None:
        """Raise if the configured "you are logged out" marker is on the page.

        Sites rarely say this with a status code -- KRX answers 400 with the
        body ``LOGOUT``, others just render the login screen again -- so the
        row has to name the tell itself: ``response_parse_json['logged_out']``,
        a css selector."""
        marker = (self.api.response_parse_json or {}).get("logged_out")
        if marker and page.query_selector(marker) is not None:
            raise SessionExpired(f"{self.api.api_id}: '{marker}' is on the page")

    def _invalidate_session(self) -> None:
        """Drop the stored cookies before logging in again -- handing an
        expired storage state to the login run itself lets the site answer it
        from the dead session instead of showing the login screen."""
        self._state_path().unlink(missing_ok=True)
