"""Self-contained v3 Playwright browser runner driven purely by ``ApiSpec``.

Zero Oracle / SQLModel / loguru dependency. Playwright is imported lazily on
first ``protocol='BROWSER'`` invocation so HTTP-only batches stay lightweight.

Supports all v2 browser capabilities:
- 12 behavior actions: ``goto``, ``fill``, ``select``, ``check``, ``click``,
  ``press``, ``wait_for``, ``wait_load``, ``sleep``, ``frame``, ``eval``,
  ``download`` + ``expect_navigation`` + dialog auto-accept.
- 4 response modes: ``dom`` (single-IPC JS extraction), ``xhr``, ``binary``,
  and ``session`` (Playwright ``storage_state`` shared with HTTP cookies).
- Pagination: ``page``, ``click``, and ``scroll`` with seam deduplication.
"""

from __future__ import annotations

import atexit
import os
import threading
import time
from contextlib import ExitStack, contextmanager
from pathlib import Path
from typing import Any, Iterator

import httpx

from app.errors import ConfigError, SessionExpired
from app.v3.http_fetcher import (
    _extract_json_records,
    _merge_page_records,
    _resolve_text,
    _rows_to_records,
    pace_host,
)
from app.v3.session_store import session_state_path
from app.v3.spec import ApiSpec

_HEADLESS = os.environ.get("BROWSER_HEADLESS", "1").lower() not in ("0", "false", "no")
_TIMEOUT_MS = int(os.environ.get("BROWSER_TIMEOUT_MS", "15000"))
_LOCALE = os.environ.get("BROWSER_LOCALE", "ko-KR")
_TIMEZONE = os.environ.get("BROWSER_TIMEZONE", "Asia/Seoul")

_local = threading.local()
_started: list[tuple[Any, Any]] = []
_started_lock = threading.Lock()

_EXTRACT_JS = """
({rowSel, fields, cellSel}) => {
  const read = (el, rule) => {
    const css  = (typeof rule === 'string') ? rule : rule.css;
    const attr = (typeof rule === 'string') ? null : rule.attr;
    const cell = css ? el.querySelector(css) : el;
    if (!cell) return null;
    const v = attr ? cell.getAttribute(attr) : cell.innerText;
    return ((v || '').trim()) || null;
  };
  const els = Array.from(document.querySelectorAll(rowSel));
  if (fields && !Array.isArray(fields)) {
    return els.map(el => {
             const rec = {};
             for (const name of Object.keys(fields)) rec[name] = read(el, fields[name]);
             return rec;
           })
           .filter(rec => Object.keys(rec).some(k => rec[k] !== null));
  }
  return els.map(el => Array.from(el.querySelectorAll(cellSel))
                        .map(c => (c.innerText || '').trim()));
}
"""


def _import_playwright():
    try:
        from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
        from playwright.sync_api import sync_playwright
    except ImportError as exc:
        raise ImportError(
            "protocol='BROWSER' requires playwright. "
            "Install with: pip install playwright && playwright install --with-deps chromium"
        ) from exc
    return sync_playwright, PlaywrightTimeoutError


def get_browser():
    browser = getattr(_local, "browser", None)
    if browser is not None and browser.is_connected():
        return browser
    sync_playwright, _ = _import_playwright()
    pw = sync_playwright().start()
    browser = pw.chromium.launch(headless=_HEADLESS)
    _local.playwright = pw
    _local.browser = browser
    with _started_lock:
        if not _started:
            atexit.register(close_browsers)
        _started.append((pw, browser))
    return browser


def close_browsers() -> None:
    with _started_lock:
        instances, _started[:] = list(_started), []
    for pw, browser in instances:
        for fn in (browser.close, pw.stop):
            try:
                fn()
            except Exception:
                pass
    _local.__dict__.pop("browser", None)
    _local.__dict__.pop("playwright", None)


def browser_state_key(api_id: str, spec: ApiSpec) -> str:
    if spec.engine.session.state:
        return str(spec.engine.session.state)
    return httpx.URL(spec.engine.request.url).host or api_id.lower()


@contextmanager
def _open_page(api_id: str, spec: ApiSpec, params: dict[str, Any]) -> Iterator[Any]:
    options: dict[str, Any] = {"locale": _LOCALE, "timezone_id": _TIMEZONE}
    st_path = session_state_path(browser_state_key(api_id, spec))
    if st_path.exists():
        options["storage_state"] = str(st_path)
    headers = {
        k: (_resolve_text(v, params) if isinstance(v, str) else v)
        for k, v in spec.engine.request.headers.items()
    }
    if headers:
        options["extra_http_headers"] = headers

    context = get_browser().new_context(**options)
    context.set_default_timeout(_TIMEOUT_MS)
    page = context.new_page()

    def _on_dialog(dialog: Any) -> None:
        try:
            dialog.accept()
        except Exception:
            pass

    page.on("dialog", _on_dialog)
    try:
        yield page
    finally:
        context.close()


def _select_frame(page: Any, step: dict[str, Any]) -> Any:
    if step.get("selector"):
        el = page.wait_for_selector(step["selector"])
        frame = el.content_frame() if el else None
    elif step.get("name"):
        frame = page.frame(name=step["name"])
    else:
        frame = page.frame(url=step["url"])
    if frame is None:
        raise ConfigError(f"no frame matched {step!r}")
    return frame


def _run_behavior(
    api_id: str,
    spec: ApiSpec,
    page: Any,
    params: dict[str, Any],
    *,
    host_policies: dict[str, dict[str, Any]] | None = None,
) -> list[tuple[str, bytes]]:
    target = page
    downloads: list[tuple[str, bytes]] = []

    start_url = _resolve_text(spec.engine.request.url, params)
    pace_host(start_url, host_policies=host_policies, api_min=spec.engine.pacing.min_interval_sec)
    page.goto(start_url, wait_until="load")

    for idx, step in enumerate(spec.engine.request.behavior or [], start=1):
        action = str(step.get("action") or "").lower()
        selector = step.get("selector")
        timeout = float(step["timeout"]) * 1000 if "timeout" in step else None

        with ExitStack() as stack:
            if step.get("expect_navigation"):
                stack.enter_context(page.expect_navigation(
                    wait_until=step.get("wait_until", "load"),
                    timeout=timeout or _TIMEOUT_MS,
                ))

            if action == "goto":
                url = _resolve_text(step.get("url") or spec.engine.request.url, params)
                pace_host(url, host_policies=host_policies, api_min=spec.engine.pacing.min_interval_sec)
                page.goto(url, wait_until=step.get("wait_until", "load"), timeout=timeout)
            elif action == "fill":
                target.fill(selector, _resolve_text(str(step.get("value", "")), params), timeout=timeout)
            elif action == "select":
                target.select_option(selector, _resolve_text(str(step.get("value", "")), params), timeout=timeout)
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
                target.wait_for_selector(selector, state=step.get("state", "visible"), timeout=timeout)
            elif action == "wait_load":
                page.wait_for_load_state(step.get("state", "networkidle"), timeout=timeout)
            elif action == "sleep":
                time.sleep(float(step.get("seconds", 1)))
            elif action == "frame":
                target = page if step.get("reset") else _select_frame(page, step)
            elif action == "eval":
                target.evaluate(_resolve_text(str(step.get("script", "")), params))
            elif action == "download":
                with page.expect_download(timeout=timeout or _TIMEOUT_MS) as info:
                    target.click(selector, timeout=timeout)
                dl = info.value
                dl_path = dl.path()
                if dl_path is None:
                    raise ConfigError(f"{api_id}: browser download failed ({dl.failure() or 'unknown'})")
                downloads.append((dl.suggested_filename, Path(dl_path).read_bytes()))
            else:
                raise ConfigError(f"{api_id}: unknown behavior action {action!r} at step {idx}")

    return downloads


def _extract_dom_rows(page: Any, parse_cfg: dict[str, Any], selector: str) -> list[dict[str, Any]]:
    rules = parse_cfg.get(selector)
    if not (isinstance(rules, dict) and "rows" in rules):
        rules = parse_cfg
    if "rows" not in rules:
        raise ConfigError(f"extract.parse requires 'rows' CSS selector for DOM extraction ({selector!r})")
    fields = rules.get("fields")
    result = page.evaluate(_EXTRACT_JS, {
        "rowSel": rules["rows"],
        "fields": fields,
        "cellSel": rules.get("cells", "td, th"),
    })
    if isinstance(fields, dict):
        return result
    return _rows_to_records([r for r in result if any(r)], rules)


def collect_browser_bundle(
    api_id: str,
    spec: ApiSpec,
    params: dict[str, Any],
    *,
    host_policies: dict[str, dict[str, Any]] | None = None,
) -> tuple[list[dict[str, Any]], dict[str, list[dict[str, Any]]], list[tuple[str, bytes]]]:
    """Execute one ``protocol='BROWSER'`` ``ApiSpec``.

    Returns ``(primary_records, extra_records_by_target, downloads)``.
    For ``response_type='session'``, saves Playwright storage state and returns empty lists.
    """
    _, PlaywrightTimeoutError = _import_playwright()
    work_params = dict(params)
    ext = spec.engine.extract
    rtype = (ext.response_type or "dom").lower()
    if rtype == "auto":
        rtype = "dom"
    pag = spec.engine.pagination
    extra_map = ext.extra_selectors or {}

    if pag is not None and pag.mode == "page":
        work_params.setdefault(pag.param, pag.start)

    try:
        with _open_page(api_id, spec, work_params) as page:
            captured: list[Any] = []
            if rtype == "xhr":
                url_pat = (ext.parse or {}).get("url") or ""

                def _on_resp(resp: Any) -> None:
                    if url_pat and url_pat not in resp.url:
                        return
                    try:
                        captured.append(resp.json())
                    except Exception:
                        pass

                page.on("response", _on_resp)

            downloads = _run_behavior(api_id, spec, page, work_params, host_policies=host_policies)

            marker = spec.engine.session.logged_out
            if marker and page.query_selector(marker) is not None:
                raise SessionExpired(f"{api_id}: logged_out selector {marker!r} matched on page")

            if rtype == "session":
                st_path = session_state_path(browser_state_key(api_id, spec))
                st_path.parent.mkdir(parents=True, exist_ok=True)
                page.context.storage_state(path=str(st_path))
                return [], {}, []

            if rtype == "binary":
                if not downloads:
                    raise ConfigError(f"{api_id}: browser behavior produced no downloads")
                return [], {}, downloads

            if rtype == "xhr":
                if not captured:
                    raise ConfigError(f"{api_id}: no XHR response matched pattern {(ext.parse or {}).get('url')!r}")
                bodies = captured if (ext.parse or {}).get("pick") == "all" else captured[-1:]
                primary: list[dict[str, Any]] = []
                extras: dict[str, list[dict[str, Any]]] = {t: [] for t in extra_map.values()}
                for body in bodies:
                    primary.extend(_extract_json_records(body, ext.selector))
                    for sel, tgt in extra_map.items():
                        extras[tgt].extend(_extract_json_records(body, sel))
                return primary, extras, downloads

            # rtype == "dom"
            parse_cfg = ext.parse or {}
            if pag is None or pag.mode not in ("page", "click", "scroll"):
                primary = _extract_dom_rows(page, parse_cfg, ext.selector)
                extras = {tgt: _extract_dom_rows(page, parse_cfg, sel) for sel, tgt in extra_map.items()}
                return primary, extras, downloads

            page_delay = spec.engine.pacing.page_delay_sec
            if page_delay is None:
                page_delay = float(os.environ.get("PAGE_DELAY_SEC", "0.3"))

            accum: list[dict[str, Any]] = []
            seen: dict[str, int] = {}
            extra_accum: dict[str, list[dict[str, Any]]] = {t: [] for t in extra_map.values()}
            extra_seen: dict[str, dict[str, int]] = {t: {} for t in extra_map.values()}
            previous: list[dict[str, Any]] | None = None

            for _ in range(1, pag.max_pages + 1):
                page_recs = _extract_dom_rows(page, parse_cfg, ext.selector)
                if not page_recs or page_recs == previous:
                    break
                _merge_page_records(accum, seen, page_recs)
                for sel, tgt in extra_map.items():
                    e_recs = _extract_dom_rows(page, parse_cfg, sel)
                    if e_recs:
                        _merge_page_records(extra_accum[tgt], extra_seen[tgt], e_recs)
                previous = page_recs

                if pag.mode == "page":
                    work_params[pag.param] = int(work_params.get(pag.param, pag.start)) + 1
                    _run_behavior(api_id, spec, page, work_params, host_policies=host_policies)
                elif pag.mode == "scroll":
                    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    try:
                        if pag.wait_for:
                            page.wait_for_selector(pag.wait_for)
                        else:
                            page.wait_for_load_state("networkidle")
                    except PlaywrightTimeoutError:
                        break
                elif pag.mode == "click":
                    ctrl = page.query_selector(pag.selector)
                    if ctrl is None or not ctrl.is_enabled() or not ctrl.is_visible():
                        break
                    ctrl.click()
                    try:
                        if pag.wait_for:
                            page.wait_for_selector(pag.wait_for)
                        else:
                            page.wait_for_load_state("networkidle")
                    except PlaywrightTimeoutError:
                        break
                if page_delay > 0:
                    time.sleep(page_delay)

            return accum, extra_accum, downloads
    except PlaywrightTimeoutError as exc:
        if spec.engine.session.login:
            raise SessionExpired(f"{api_id}: {str(exc).splitlines()[0]}") from exc
        raise
