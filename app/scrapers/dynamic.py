"""Config-driven scraping engine: builds and runs an HTTP request purely from
an ``ApiMst`` row (no per-site subclass needed), then extracts result
records generically as ``{selector: [record, ...]}``.

Unlike a hand-written scraper (one Python class per site), this engine reads
everything it needs -- method, url, headers, payload, and which XML/JSON tag
holds each result record -- from the DB, so adding a new source is a matter
of inserting an ``ApiMst`` row rather than writing code.

This module only covers fetch + parse. What happens to the parsed records
(saving to a table, buffering/exporting to Parquet, job bookkeeping) is
:mod:`app.services.export` and :mod:`app.services.execution`.
"""

from __future__ import annotations

import atexit
import csv
import io
import json
import os
import re
import ssl
import threading
import time
import xml.etree.ElementTree as ET
import zipfile
from pathlib import Path
from typing import Any

import certifi
import httpx
from loguru import logger
from tenacity import (retry, retry_if_exception, stop_after_attempt,
                      wait_exponential)

from app.auth_config import persist_env_var, resolve_env_placeholders
from app.errors import MappingError, ParamError, SessionExpired, SourceRefused, classify
from app.scrapers.base import BaseScraper, merge_records, rows_to_records

# payload_type is stored verbatim as the httpx request kwarg name, so each
# ApiMst row picks its own transport mechanism purely via data -- no code
# change needed to add a form-urlencoded ('data') site alongside XML
# ('content') and JSON ('json') ones. None means no body at all (typical GET).
_TEXT_PAYLOAD_KWARGS = {"content"}
_DICT_PAYLOAD_KWARGS = {"data", "json"}

# Seconds to sleep between page fetches during pagination (see
# fetch_all_pages) -- one value for every site, configured via .env rather
# than per-API DB config (pagination politeness isn't something that varies
# site-to-site enough here to need its own DB column).
_PAGE_DELAY_SEC = float(os.environ.get("PAGE_DELAY_SEC", "0.3"))

# Reserved param name for pagination: an ApiMst row opts into paging simply by
# putting ':PAGE' somewhere in api_url/payload_xml (e.g. DART's
# '&page_no=:PAGE') -- no separate config field needed, same as any other
# ':KEY' placeholder except this one is driven by the fetch loop, not params_json.
_PAGE_PARAM = "PAGE"

# Matches a bare ':PAGE' token -- the marker that means page-number paging
# when an ApiMst row predates pagination_json.
PAGE_MARKER_RE = r"(?<!\w):" + _PAGE_PARAM + r"(?!\w)"

# Minimum seconds between two requests to the *same host*, enforced
# process-wide by _pace_host (see fetch). Distinct from _PAGE_DELAY_SEC,
# which only spaces the pages within one paginated fetch -- this spans every
# job the process runs, which is what a per-account rate limit actually
# measures. Sized against KIS, measured live: ~8 req/s is where it starts
# rejecting with EGW00201 ("초당 거래건수를 초과하였습니다", returned as HTTP
# 500), so 0.15s (6.7 req/s) keeps ~15% margin under that while staying
# inside KIS's own documented 100~150ms guidance. Per-host rather than
# global so one site's limit doesn't throttle every other site a process
# happens to scrape in the same run.
_REQUEST_MIN_INTERVAL_SEC = float(os.environ.get("REQUEST_MIN_INTERVAL_SEC", "0.15"))

# Per-host overrides, {host_suffix: seconds}, consulted before the default
# above (see _interval_for). Settable as JSON in REQUEST_MIN_INTERVAL_HOSTS so
# a site can be slowed down without a release.
#
# seibro.or.kr is here because the bond collection is the one job in this
# process that hammers a site nobody metered for us. It runs ~10 requests per
# instrument against a WebSquare screen, for an hour a night, for weeks --
# a shape that looks nothing like the burst-then-idle traffic the KIS-derived
# default was measured for. 0.5s holds it to 2 req/s.
_HOST_INTERVALS: dict[str, float] = {
    "seibro.or.kr": 0.5,
    # KRX 표준코드 검색 화면. seibro 와 같은 이유로 같은 값이고, 접미사
    # 일치라 seibro 항목이 이 호스트를 덮어주지 않아 따로 적는다. 브라우저
    # 잡도 이 표를 그대로 쓴다 (app.scrapers.browser 가 _pace_host 를
    # 공유한다) -- 한 화면을 여는 데 요청이 여러 건 나가는 쪽이라 오히려
    # 간격이 더 중요하다.
    "isin.krx.co.kr": 0.5,
    # 오픈 API. 인증키 하나당 하루 10,000회가 한도이고(약관 명시, 초과 시
    # 서비스 중단), 응답에는 남은 횟수를 알려주는 헤더가 없다. 과거분을
    # 훑을 때 초당 몇 건이 나가는지가 그대로 하루치 소모가 되므로, 실측
    # (지수 백필 1.6 req/s, 4,349건 무사고)보다 느리게 잡아 둔다.
    "data-dbg.krx.co.kr": 0.3,
    # 데이터마켓(로그인 필요)도 같은 성격이다. ETF 구성종목은 종목당 한
    # 요청이라 하루 1,400건 안팎이 한 줄로 나간다. 0.5 였을 때 2 req/s 로
    # 몰린 직후 에러페이지로 막혔다 -- 2026-09-17 21:09 에 1분 115건 뒤,
    # 09-14 에 10분 303건 뒤. 반면 09-17 새벽엔 10분 300~600건(0.5~1 req/s)
    # 으로 세 시간을 무사히 갔다. 그 사이 어딘가가 문턱이라 1초로 잡는다:
    # 1,400건이면 24분이고, 막혀서 밤을 잃는 것보다 싸다.
    "data.krx.co.kr": 1.0,
    # 0.2s 로 43개 ETF 를 훑었더니 22개가 JSON 이 아닌 응답으로 돌아왔다 --
    # 오류 코드가 아니라 본문이 바뀌는 형태라, 세는 쪽이 아니라 받는 쪽이
    # 밀린 것으로 보인다. 여기도 metered API 가 아니다.
    "samsungfund.com": 0.5,
    # 나머지 자산운용사도 같은 이유로 묶는다. ETF 구성종목은 종목마다 한
    # 요청이라 하루 한 번 수십~수백 건이 한 줄로 나가는데, 어느 곳도 우리를
    # 위해 용량을 재준 적이 없는 상품 소개 사이트다. 삼성만 예외로 두면
    # "아직 아무도 항의하지 않은 곳"과 "괜찮은 곳"이 구별되지 않는다.
    "aceetf.co.kr": 0.5,
    "soletf.co.kr": 0.5,
    "plusetf.co.kr": 0.5,
    "riseetf.co.kr": 0.5,
    "hanaroetf.com": 0.5,
    "kiwoometf.com": 0.5,
    "timeetf.co.kr": 0.5,
    "investments.miraeasset.com": 0.5,
    **json.loads(os.environ.get("REQUEST_MIN_INTERVAL_HOSTS", "{}")),
}

# One httpx.Client shared by every DynamicApiScraper in the process. A new
# client per request (what this replaced) meant a fresh TLS handshake every
# time: measured live against KIS, 287ms per request of which only ~24ms was
# the actual response -- i.e. ~92% of each request was handshake. Keeping one
# client alive across jobs removes that entirely. A scraper instance is built
# fresh per ApiJob (see app.services.execution.run_job), so the pool has to
# live at module level, not on the instance, to survive across jobs.
# httpx.Client is thread-safe.
_client: httpx.Client | None = None
_client_lock = threading.Lock()

# Per-host timestamp of the next free request slot (monotonic clock).
_pace_lock = threading.Lock()
_next_slot_at: dict[str, float] = {}


# Extra CA certificates, one PEM per file, added to certifi's bundle for
# every request this process makes (see _ssl_context).
_EXTRA_CA_DIR = Path(__file__).resolve().parent.parent / "certs"


def _ssl_context() -> ssl.SSLContext:
    """certifi's trust store plus whatever sits in app/certs.

    Some servers send an incomplete chain -- kiwoometf.com presents only its
    leaf and leaves out the Sectigo intermediate that signs it -- so
    verification fails on a certificate that is perfectly valid. Browsers
    paper over this by fetching the missing link from the leaf's AIA
    extension; httpx does not, and neither should we at request time (it
    would mean trusting a URL read out of the certificate being verified).

    Carrying the intermediate ourselves settles it before the connection:
    it is a public CA certificate whose own issuer (USERTrust RSA) is
    already a certifi root, so nothing is trusted here that Sectigo could
    not already vouch for. Verification stays on -- the alternative,
    verify=False, would drop it for the whole host."""
    context = ssl.create_default_context(cafile=certifi.where())
    for pem in sorted(_EXTRA_CA_DIR.glob("*.pem")):
        context.load_verify_locations(cafile=str(pem))
    return context


def get_http_client() -> httpx.Client:
    """The process-wide connection-pooled client (see _client)."""
    global _client
    with _client_lock:
        if _client is None or _client.is_closed:
            _client = httpx.Client(timeout=30, follow_redirects=True,
                                   verify=_ssl_context())
            atexit.register(close_http_client)
        return _client


def close_http_client() -> None:
    """Release the shared pool. Registered with atexit, so neither entrypoint
    (one-shot CLI process, long-running API app) needs to call it by hand."""
    global _client
    with _client_lock:
        if _client is not None and not _client.is_closed:
            _client.close()
        _client = None


def _interval_for(host: str) -> float:
    """This host's minimum gap between requests.

    The default above is sized against KIS's published per-second limit, which
    is the wrong yardstick for a site that publishes no limit at all: SEIBRO
    and KRX's ISIN service are web screens, not metered APIs, and what is
    polite there is a judgement rather than a number they gave us. A host
    listed here uses its own gap; everything else uses the default.

    Matched on suffix so one entry covers a site's subdomains
    (``seibro.or.kr`` also covers ``isin.krx.co.kr``-style hosts only if
    listed separately -- this is a suffix match, not a guess at ownership).
    """
    for suffix, interval in _HOST_INTERVALS.items():
        if host == suffix or host.endswith("." + suffix):
            return interval
    return _REQUEST_MIN_INTERVAL_SEC


def _pace_host(url: str, api_min: float | None = None) -> None:
    """Block until this host's next request slot is due, so a whole cycle's
    worth of jobs can't burst past the site's rate limit. Reserves the slot
    while holding the lock and only *then* sleeps, so concurrent callers each
    get their own slot instead of all waking on the same one.

    ``api_min`` (ApiMst.options_json['min_interval_sec']) 은 호스트 간격을
    **대체하지 않고 바닥을 올린다** -- 둘 중 큰 값을 쓴다.

    이유는 방향이다: API 쪽 값으로 **느려지는 것만** 허용한다. _HOST_INTERVALS
    의 값은 추측이 아니라 맞아 본 결과다 -- data.krx.co.kr 1.0s 는 1분 115건
    뒤 에러페이지로 막힌 뒤에 정한 것이고, samsungfund.com 0.5s 는 0.2s 로
    43개를 훑다가 22개가 깨진 뒤에 정한 것이다. 대체를 허용하면 설정 한 줄로
    그 교훈을 되돌릴 수 있고, 되돌린 줄도 모른다.

    (슬롯은 호스트별로 하나이므로 API 가 여러 개여도 연속 요청은 이미 간격만큼
     떨어진다 -- 같은 호스트의 API 수가 간격을 합산하지는 않는다. 실행도
     직렬이다(run_cycle 이 잡을 하나씩 돈다). 병렬이 생기는 것은 별개
     프로세스가 겹칠 때뿐이고, 그때를 위해 슬롯이 프로세스 단위로 공유된다.)
    """
    host = httpx.URL(url).host or ""
    interval = _interval_for(host)
    if api_min is not None:
        interval = max(interval, api_min)
    if interval <= 0:
        return
    with _pace_lock:
        now = time.monotonic()
        slot = max(now, _next_slot_at.get(host, 0.0))
        _next_slot_at[host] = slot + interval
    time.sleep(max(0.0, slot - time.monotonic()))


def _response_body(response: httpx.Response) -> Any:
    """The reply as data for continuation lookups, or {} when it isn't JSON.

    Continuation values live in the body for some APIs and in headers for
    others; only the body needs decoding, and a reply that isn't JSON simply
    has none to offer rather than being an error here."""
    try:
        return response.json()
    except Exception:
        return {}


def _normalize_xml_selector(selector: str) -> str:
    """ElementTree's own (limited) XPath syntax is used as-is when the
    selector already looks like a path (starts with '.' or contains '/'),
    e.g. 'vector/data/result', './/detail/result', "result[@type='x']".

    A bare dot-separated selector with no '/' is shorthand searched at any
    depth: 'vector.data.result' -> './/vector/data/result', and a single
    bare tag name 'result' -> './/result' (search the whole document,
    matching the old flat-tag behavior)."""
    if selector.startswith(".") or "/" in selector:
        return selector
    return f".//{selector.replace('.', '/')}"


def _parse_selector(selector: str) -> tuple[str, str | None]:
    """Split an optional explicit '#attr' suffix off a selector, e.g.
    'result#code' -> ('result', 'code'). When present, this overrides
    _child_value's auto-detection with a specific attribute name -- mainly
    for the case _child_value can't resolve on its own (a child with
    multiple attributes and none of them named 'value')."""
    if "#" in selector:
        path, _, attr = selector.partition("#")
        return path, attr
    return selector, None


def _group_children(el: ET.Element, preferred_attr: str | None = None) -> dict[str, Any]:
    """Build ``{tag: value}`` from ``el``'s direct children. A tag that
    repeats among siblings becomes a *list* of values instead of the last
    occurrence silently overwriting the earlier ones."""
    grouped: dict[str, list[Any]] = {}
    for child in el:
        grouped.setdefault(child.tag, []).append(_child_value(child, preferred_attr))
    return {tag: (values[0] if len(values) == 1 else values) for tag, values in grouped.items()}


def _child_value(child: ET.Element, preferred_attr: str | None = None) -> Any:
    """A child element's value. If it has its own sub-elements, recurse via
    ``_group_children`` instead of collapsing them to ``None`` -- this is
    what makes nesting *and* repeated tags work at any depth, since
    ``_group_children`` calls back into this function per grandchild.

    Otherwise it's a scalar: if ``preferred_attr`` is given (via a '#attr'
    selector suffix) and present, it wins outright. Otherwise, no fixed
    attribute name is assumed: prefer a ``value`` attribute (SEIBRO-style
    ``<ISIN value="x"/>``, kept first for backward compatibility), then any
    single attribute regardless of its name (``<ISIN val="x"/>``), then
    element text content (``<ISIN>x</ISIN>``). Multiple attributes with no
    ``value`` among them and no ``preferred_attr`` given is genuinely
    ambiguous, so nothing is silently dropped -- the whole attrib dict is
    returned instead of guessing which one is "the" value."""
    if list(child):
        return _group_children(child, preferred_attr)
    if preferred_attr and preferred_attr in child.attrib:
        return child.get(preferred_attr)
    if "value" in child.attrib:
        return child.get("value")
    if len(child.attrib) == 1:
        return next(iter(child.attrib.values()))
    if len(child.attrib) > 1:
        return dict(child.attrib)
    if child.text and child.text.strip():
        return child.text.strip()
    return None


def _extract_xml_records(root: ET.Element, selector: str) -> list[dict[str, Any]]:
    path, preferred_attr = _parse_selector(selector)
    records = []
    for el in root.findall(_normalize_xml_selector(path)):
        if list(el):
            # e.g. <result><ISIN value="x"/><NAME>y</NAME></result>
            records.append(_group_children(el, preferred_attr))
        else:
            # fields as attributes directly on the record tag
            records.append(dict(el.attrib))
    return records


def _extract_json_records(data: Any, selector: str) -> list[dict[str, Any]]:
    """Dot-notation path into a JSON document, e.g. 'data.items' ->
    data["data"]["items"]. A dict at the target path is wrapped as a
    single-record list; missing/non-dict intermediate steps yield [].

    ``"."`` means the document already *is* the record list -- some APIs
    answer with a bare array and no envelope at all (KODEX's product list).
    Spelled the same way as for XML, where ``"."`` picks the root element
    itself rather than a descendant of it, so one convention covers both."""
    if selector == ".":
        return list(data) if isinstance(data, list) else ([data] if isinstance(data, dict) else [])

    for part in selector.split("."):
        if not isinstance(data, dict):
            return []
        data = data.get(part)
    if data is None:
        return []
    if isinstance(data, dict):
        data = [data]
    return list(data)


def _extract_json_scalar(data: Any, path: str) -> Any:
    """Same dot-notation traversal as _extract_json_records, but returns the
    raw value at that path as-is (no list-wrapping) -- for merge_fields_json
    and persist_env_json, which each want a single value, not a record list."""
    for part in path.split("."):
        if not isinstance(data, dict):
            return None
        data = data.get(part)
    return data


class DynamicApiScraper(BaseScraper):
    """Builds and runs one HTTP call described by an ``ApiMst`` row.

    ``api.payload_type`` is the httpx request kwarg to send the body as --
    ``None`` (no body, typical for GET), ``'content'`` (raw text from
    ``payload_xml``, e.g. XML), ``'data'`` (form-urlencoded dict from
    ``payload_json``), or ``'json'`` (JSON dict from ``payload_json``) -- so
    each site picks its own transport purely via its ``ApiMst`` row, no code
    change required.

    ``params`` values are substituted as ``:KEY`` placeholders (Oracle
    bind-variable style, e.g. ``:KACD``, matched as a whole token) into
    ``api_url``, ``header_json`` values, and ``payload_xml`` -- consistently,
    everywhere a param could plausibly need to sit mid-string. Dict payloads
    (``data``/``json``) are the one exception: ``params`` is shallow-merged
    on top of ``payload_json`` by key instead, since a JSON/form body is
    already flat key-value pairs and has no "mid-string" case to support.

    ``api_url``, ``header_json``, ``payload_json``, and ``payload_xml``
    values may reference an env var as ``"${VAR_NAME}"`` (e.g. an API key
    stays in ``.env``, not in the DB row -- some APIs put it in a header,
    others (like DART) in the URL's query string, others (like KIS's OAuth
    token endpoint) in the JSON body, others in an XML/SOAP body) --
    resolved from ``os.environ`` at request-build time.

    A ``:PAGE`` placeholder anywhere in ``api_url``/``payload_xml`` opts into
    pagination: :meth:`collect` then calls :meth:`fetch_all_pages`
    instead of a single :meth:`fetch`, incrementing ``:PAGE`` from 1 and
    stopping once a page comes back with no records for any selector.

    Saving what comes back is not this class's business -- see
    :class:`app.scrapers.base.BaseScraper`, which this shares with the
    browser-driven transport.
    """

    # ---- request building --------------------------------------------------

    def build_request(self) -> dict[str, Any]:
        api = self.api
        headers = {
            k: (resolve_env_placeholders(self._substitute_placeholders(v)) if isinstance(v, str) else v)
            for k, v in (api.header_json or {}).items()
        }
        kwargs: dict[str, Any] = {
            "method": (api.method or 'GET').upper(),
            "url": resolve_env_placeholders(self._substitute_placeholders(api.api_url)),
            "headers": headers,
        }

        if api.payload_type is None:
            pass  # no body -- typical for GET
        elif api.payload_type in _TEXT_PAYLOAD_KWARGS:
            # raw text body (XML, pre-built text, ...) -- :KEY params first,
            # then ${VAR} env placeholders, same as headers/url/payload_json
            # (e.g. a SOAP/XML body that needs to embed a secret).
            text_payload = resolve_env_placeholders(self._substitute_placeholders(api.payload_xml or ""))
            kwargs[api.payload_type] = text_payload.encode("utf-8")
        elif api.payload_type in _DICT_PAYLOAD_KWARGS:
            # dict-shaped body ('data' = form-urlencoded, 'json' = JSON) --
            # env placeholders resolved the same as headers/url (e.g. KIS's
            # OAuth token endpoint needs appkey/appsecret in the body itself).
            payload = {**(api.payload_json or {}), **self.params}
            kwargs[api.payload_type] = {
                k: (resolve_env_placeholders(v) if isinstance(v, str) else v) for k, v in payload.items()
            }
        else:
            valid = sorted({None, *_TEXT_PAYLOAD_KWARGS, *_DICT_PAYLOAD_KWARGS}, key=str)
            raise ParamError(f"{api.api_id}: payload_type must be one of {valid}, "
                             f"got '{api.payload_type}'")

        return kwargs

    # ---- execution ------------------------------------------------------

    # 재시도할지는 **예외의 등급**이 정한다 (app.errors.ScrapeError.retryable).
    # 전에는 여기에 "SessionExpired 와 SourceRefused 만 빼고 전부 재시도" 라고
    # 적혀 있었다. 두 제외의 이유는 그대로다: 로그아웃은 같은 요청을 세 번 더
    # 보내서 풀리지 않고 백오프 10초만 쓴다(재로그인은 collect 가 한다), 거절은
    # 반대 이유로 -- 사이트가 원하는 건 요청을 덜 받는 것이다.
    #
    # 바뀐 것은 **설정 오류**다. build_request 가 올리는 ParamError 는 전에
    # 세 번 재시도하고 10초를 썼는데, 다시 만들어도 같은 설정이라 같은 예외다.
    # 이제 ConfigError 등급이 retryable=False 라 바로 실패한다. fetch 의
    # 범위 안에서 날 수 있는 것은 요청 생성 · httpx · 마커 · 상태코드뿐이고
    # (파싱은 collect 쪽이라 이 retry 밖이다) 나머지는 전부 SourceUnavailable
    # 로 분류돼 전과 같이 재시도된다.
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=10),
           retry=retry_if_exception(lambda exc: classify(exc).retryable))
    def fetch(self) -> httpx.Response:
        request_kwargs = self.build_request()
        # Paced *inside* the retry, so a retried attempt (e.g. after a rate-
        # limit rejection) takes its own slot rather than firing immediately.
        _pace_host(request_kwargs["url"], self._option_float("min_interval_sec"))
        # 요청별 타임아웃. 공용 클라이언트의 기본값(30)을 이 행만 덮는다 --
        # 클라이언트가 프로세스 하나뿐이라 클라이언트 쪽에 둘 수 없다.
        #
        # 최악을 묶는 유일한 손잡이다. httpx 의 timeout 은 connect·read 각각에
        # 걸리므로 한 시도가 최대 2배이고, 재시도가 3회라 한 잡이 6배까지 간다
        # -- 기본값이면 3분이다. KB 가 2026-10-03 에 ETF 사이트를 옮기고 옛
        # 호스트가 "연결이 안 되는 곳" 으로 302 를 돌려줬을 때 잡마다 정확히 그
        # 3분을 먹었고, 10-12 밤 사이클은 852개를 들고 다섯 시간을 쓰고 성공
        # 둘로 끝났다. 운용사 사이트처럼 빨라야 정상인 곳은 10초면 충분하다.
        timeout = self._option_float("timeout_sec")
        if timeout is not None:
            request_kwargs["timeout"] = timeout
        response = get_http_client().request(**request_kwargs)
        self._check_reply_markers(response)
        response.raise_for_status()
        return response

    def _check_reply_markers(self, response: httpx.Response) -> None:
        """Raise if this reply is the site saying "you are not logged in"
        (SessionExpired) or "go away" (SourceRefused), by the markers the row
        configured. The two are told apart by what the caller must do next;
        see the exception classes.

        Checked *before* raise_for_status because that is often how it
        arrives: KRX's data endpoint answers HTTP 400 with the body ``LOGOUT``
        rather than a status a client could read on its own. So the row names
        the tell itself -- ``session_json['logged_out']``, a string
        that appears in the body (a css selector for a browser row; same key,
        same job, read the way that transport can read it).

        Rows that do not configure it pay a dict lookup and nothing else. That
        matters here more than anywhere: every API in the process goes through
        fetch(), including a 3-minute KIS cycle pushing several requests a
        second, so this check has to cost nothing for the rows it does not
        concern. Scoped by configuration rather than by host, which is
        tighter -- only the rows that asked are ever examined."""
        cfg = self.api.session_json or {}
        marker, blocked = cfg.get("logged_out"), cfg.get("blocked")
        if not marker and not blocked:
            return
        # Cheap guard for a row that also downloads documents: a marker only
        # ever appears in a text reply, and a truncated read is enough.
        head = response.content[:4]
        body = "" if head[:2] == b"PK" or head == b"%PDF" else response.text[:4000]
        if marker and marker in body:
            raise SessionExpired(f"{self.api.api_id}: reply says {marker!r}")
        # The refusal page is checked the same way and named the same way --
        # ``response_parse_json['blocked']``, a string that appears in it.
        # KRX's is an HTML page whose title says 에러페이지; left unrecognised
        # it reached the JSON parser and failed every job as "mismatched
        # tag", ten thousand times a night, with nothing slowing down.
        if blocked and blocked in body:
            raise SourceRefused(f"{self.api.api_id}: reply is the site's refusal page ({blocked!r})",
                                host=httpx.URL(str(response.url)).host or "")

    # ---- continuation ----------------------------------------------------

    def _pagination_spec(self) -> dict[str, Any] | None:
        """How this API says "there is more", or None if one request is all.

        An explicit ``pagination_json`` wins. Otherwise a ':PAGE' marker in
        the url/payload still means page-number paging, so rows written
        before that column existed keep working untouched."""
        if self.api.pagination_json:
            return self.api.pagination_json
        # api_url/payload_xml use :TOKEN string substitution, so a literal
        # ':PAGE' anywhere in them is the marker. payload_json instead uses
        # key-overlay substitution (see build_request), so there a 'PAGE'
        # *key* is that mechanism's own marker.
        template = (self.api.api_url or "") + (self.api.payload_xml or "")
        if re.search(PAGE_MARKER_RE, template) or _PAGE_PARAM in (self.api.payload_json or {}):
            return {"mode": "page", "param": _PAGE_PARAM}
        return None

    def is_paginated(self) -> bool:
        return self._pagination_spec() is not None

    @staticmethod
    def _extract_path(data: Any, path: str) -> list[Any]:
        """Values at a dotted path, always as a list.

        A '[]' suffix steps into a list and keeps going for every element, so
        "output2[].stck_cntg_hour" collects one timestamp per bar. A missing
        segment yields nothing rather than raising -- a continuation field
        being absent is how several APIs say "that was the last page"."""
        current: list[Any] = [data]
        for segment in path.split("."):
            explode = segment.endswith("[]")
            key = segment[:-2] if explode else segment
            nxt: list[Any] = []
            for item in current:
                if not isinstance(item, dict):
                    continue
                value = item.get(key)
                if value is None:
                    continue
                nxt.extend(value if explode and isinstance(value, list) else [value])
            current = nxt
        return current

    def _read_source(self, source: str, response: httpx.Response, body: Any) -> Any:
        """One continuation value, from a response header or the body.

        Headers are addressed as "header:<name>" because that is where a good
        few APIs keep continuation state -- KIS returns tr_cont and
        ctx_area_* there -- leaving a plain string to mean a body path."""
        if source.startswith("header:"):
            return response.headers.get(source[len("header:"):])
        values = self._extract_path(body, source)
        return values[0] if values else None

    def _advance(self, spec: dict[str, Any], response: httpx.Response,
                 body: Any, record_count: int) -> bool:
        """Set up the next request, or return False when the result is done."""
        mode = spec.get("mode", "page")

        if mode == "page":
            param = spec.get("param", _PAGE_PARAM)
            self.params[param] = int(self.params.get(param, 0)) + 1
            return True

        if mode == "cursor":
            # A full reply suggests more remains; a short one means the series
            # ran out. Without this an API that keeps answering from the same
            # anchor would run to max_pages.
            minimum = spec.get("min_records")
            if minimum is not None and record_count < int(minimum):
                return False
            values = self._extract_path(body, spec["from"])
            if not values:
                return False
            cursor = min(values) if spec.get("pick", "min") == "min" else max(values)
            param = spec["param"]
            if self.params.get(param) == cursor:
                # The anchor did not move; following it again would re-fetch
                # the same window forever.
                return False
            self.params[param] = cursor
            return True

        if mode == "token":
            condition = spec.get("continue_when") or {}
            if condition:
                actual = self._read_source(condition["source"], response, body)
                if actual not in condition.get("in", []):
                    return False
            for param, source in (spec.get("params") or {}).items():
                value = self._read_source(source, response, body)
                if value is None:
                    return False
                self.params[param] = value
            return True

        raise ValueError(f"{self.api.api_id}: unknown pagination mode {mode!r}")

    def fetch_all_pages(self, delay: float | None = None) -> dict[str, list[dict[str, Any]]]:
        """Keep requesting until the result is complete, merging every reply.

        Stops on whichever comes first: a reply with no records, a reply
        identical to the one before (some APIs clamp an out-of-range request
        to the last valid page rather than answering empty, and would
        otherwise be re-saved forever), the mode's own signal that nothing
        remains, or ``max_pages`` as a backstop for a stop condition that
        never fires.

        ``delay`` (seconds between requests) defaults to ``PAGE_DELAY_SEC``;
        it is politeness layered on the per-host rate pacing in fetch()."""
        spec = self._pagination_spec() or {}
        # 인자로 준 것이 가장 세고, 그다음이 이 행의 설정, 마지막이 기본값이다.
        if delay is None:
            delay = self._option_float("page_delay_sec")
        delay = _PAGE_DELAY_SEC if delay is None else delay
        max_pages = int(spec.get("max_pages", 100))
        if spec.get("mode", "page") == "page":
            self.params[spec.get("param", _PAGE_PARAM)] = int(spec.get("start", 1))

        all_records: dict[str, list[dict[str, Any]]] = {}
        seen: dict[str, dict[str, int]] = {}
        previous: dict[str, list[dict[str, Any]]] | None = None

        for attempt in range(1, max_pages + 1):
            response = self.fetch()
            per_selector = self.parse(response)
            count = sum(len(r) for r in per_selector.values())
            logger.info("{}: request {} -> {} record(s)", self.api.api_id, attempt, count)

            if count == 0:
                break
            if per_selector == previous:
                logger.info("{}: request {} repeats the previous reply, stopping",
                            self.api.api_id, attempt)
                break

            # A cursor anchored on a value from the last reply re-requests the
            # record that value came from, so that value is what identifies a
            # record across the seam rather than the record as a whole -- see
            # app.scrapers.base.merge_records for why that matters.
            cursor_field = spec.get("from", "").split(".")[-1] if spec.get("mode") == "cursor" else None
            merge_records(all_records, seen, per_selector, cursor_field)
            previous = per_selector

            if not self._advance(spec, response, _response_body(response), count):
                break
            time.sleep(delay)

        return all_records

    # ---- response parsing -----------------------------------------------

    def parse(self, response: httpx.Response) -> dict[str, list[dict[str, Any]]]:
        """Run every selector in ``record_map_json`` against the response,
        returning ``{selector: [record, ...]}``. For JSON responses,
        ``merge_fields_json`` (if set) then injects extra fields sourced from
        elsewhere in the same response into those records, and
        ``persist_env_json`` (if set) persists fields to ``.env`` -- see
        ``ApiMst.merge_fields_json``/``ApiMst.persist_env_json`` and
        :meth:`_apply_merge_fields`/:meth:`_persist_env_fields`."""
        mapping = self.api.record_map_json or {}
        if not mapping and not self.api.persist_env_json:
            raise ValueError(f"{self.api.api_id}: record_map_json is empty")

        if self.api.response_type == "binary":
            # The document *is* the result -- nothing to extract from it. It
            # goes to the staging area whole and what comes back is a single
            # record describing it, so the rest of the pipeline (job logging,
            # save_mode, the result table) works unchanged; only the bytes
            # take a different route. See app.services.export.stage_file.
            staged = self._stage_binary(response.content,
                                        response.headers.get("content-type"))
            return {selector: staged for selector in mapping}

        if self.api.response_type == "html_table":
            # Content-type says spreadsheet and the body is markup, so this
            # has to be decided by config rather than sniffed.
            records = self._parse_html_table(response)
            return {selector: records for selector in mapping}

        if self.api.response_type == "html_block":
            records = self._parse_html_block(response)
            return {selector: records for selector in mapping}

        if self.api.response_type in ("xlsx", "xls"):
            records = self._parse_spreadsheet(response)
            return {selector: records for selector in mapping}

        if self.api.response_type in ("zip_delimited", "delimited"):
            # Binary/plain-text response, not XML/JSON -- response.text may
            # be garbage (zip) or just isn't XML/JSON, so this must be
            # checked (and handled) before any content-type sniffing below.
            # No real "selector" concept applies to a flat delimited file
            # (unlike an XML/JSON path into a nested response) -- every key
            # in record_map_json just gets the same full record list,
            # same as a single selector normally would if there were only one.
            if self.api.response_type == "zip_delimited":
                records = self._parse_zip_delimited(response)
            else:
                records = self._parse_delimited(response)
            return {selector: records for selector in mapping}

        content_type = response.headers.get("content-type", "")
        # response shape is independent of the request's payload_type, so it's
        # judged from the response itself: content-type header first, falling
        # back to sniffing the body for servers that mislabel it.
        looks_like_xml = "xml" in content_type or response.text.lstrip().startswith("<")

        if looks_like_xml:
            root = ET.fromstring(response.text)
            return {selector: _extract_xml_records(root, selector) for selector in mapping}

        data = response.json()
        per_selector = {selector: _extract_json_records(data, selector) for selector in mapping}
        self._apply_merge_fields(data, per_selector)
        self._persist_env_fields(data)
        return per_selector

    def _parse_zip_delimited(self, response: httpx.Response) -> list[dict[str, Any]]:
        """response_type='zip_delimited': unzip response.content in-memory,
        decode the configured inner file (response_parse_json['inner_file'],
        default: the zip's first entry), then hand the text to
        _parse_delimited_text. Verified live against fo_idx_code_mts.mst.zip
        ('|'-delimited, cp949, 9 columns/line, no header)."""
        spec = self.api.parse_json or {}
        encoding = spec.get("encoding", "utf-8")

        zf = zipfile.ZipFile(io.BytesIO(response.content))
        inner_file = spec.get("inner_file") or zf.namelist()[0]
        text = zf.read(inner_file).decode(encoding)
        return self._parse_delimited_text(text, spec)

    def _parse_delimited(self, response: httpx.Response) -> list[dict[str, Any]]:
        """response_type='delimited': the response body itself (no zip) is
        the delimited text -- a plain .csv/.txt download. Same
        parse_json shape as zip_delimited, minus 'inner_file'."""
        spec = self.api.parse_json or {}
        encoding = spec.get("encoding", "utf-8")
        text = response.content.decode(encoding)
        return self._parse_delimited_text(text, spec)

    @staticmethod
    def _parse_delimited_text(text: str, spec: dict[str, Any]) -> list[dict[str, Any]]:
        """Shared by _parse_zip_delimited/_parse_delimited: split delimited
        text into records. Uses csv.reader (not a naive str.split) so a real
        CSV's quoted fields (a delimiter or newline inside a quoted value)
        are handled correctly -- it degrades to plain splitting when the
        text has no quoting, so this is also what parsed the unquoted,
        pipe-delimited KRX master file.

        response_parse_json keys:
        - "delimiter": default ",".
        - "fields": explicit column names, in order. Required unless
          "has_header" is set.
        - "has_header": if true, the first line is a header row -- used as
          "fields" if that's not given, and always skipped from the data.
        - blank fields become None rather than "" (e.g. KRX pads
          "not applicable" with a lone space)."""
        delimiter = spec.get("delimiter", ",")
        has_header = spec.get("has_header", False)
        fields = spec.get("fields")

        rows = [row for row in csv.reader(io.StringIO(text), delimiter=delimiter) if row]
        return rows_to_records(rows, spec)

    def _parse_html_table(self, response: httpx.Response) -> list[dict[str, Any]]:
        """response_type='html_table': a table in an HTML document.

        Several managers publish holdings as an "Excel download" that is
        really an HTML table with a spreadsheet MIME type (TIGER and KB both
        answer application/vnd.ms-excel with markup in the body). The data is
        rectangular and complete; only the wrapper pretends otherwise.

        response_parse_json adds "table_index" (default 0) for a document
        with more than one table.
        """
        spec = self.api.parse_json or {}
        from bs4 import BeautifulSoup

        soup = BeautifulSoup(response.text, "html.parser")
        tables = soup.find_all("table")
        if not tables:
            return []
        table = tables[int(spec.get("table_index", 0))]
        rows = [[cell.get_text(" ", strip=True) for cell in tr.find_all(["td", "th"])]
                for tr in table.find_all("tr")]
        return rows_to_records([r for r in rows if any(r)], spec)

    def _parse_html_block(self, response: httpx.Response) -> list[dict[str, Any]]:
        """response_type='html_block': 자르기만 하고 읽지 않는다.

        ``parse_json['blocks']`` 에 맞는 요소를 **통째로** 한 레코드씩 담는다.
        필드를 뽑지 않는 것이 요점이다 -- 어떤 페이지는 수집기가 해석할 모양이
        아니고, 그때 해석을 아래층으로 미루는 것이 맞다. 이 모드가 하는 일은
        "어디를 자를지" 뿐이고 그 판단도 코드가 아니라 설정에 있다.

        ``html_table`` 과 가르는 이유: 그쪽은 사각형을 가정하고
        ``rows_to_records`` 로 위치를 맞춘다. 라벨/값 격자에서는 그 가정이
        깨진다 -- KRX 표준코드 상세 페이지의 한 행은
        ``['발행기관코드','06757','발행기관명','엔브이에이치코리아(주)']`` 이라
        라벨이 데이터 자리에 들어가고, 한 표 안에서도 열 수가 2·3·4·5 로
        섞인다.

        설정 키 둘::

            {"blocks": "table:not(:has(table)):has(td:not(:empty))",
             "label":  "th"}

        ``blocks`` 는 요소 종류를 가리지 않는다 -- ``table`` 이든 ``div.card``
        이든 ``ul#result > li`` 이든 같다. 그래서 "래퍼 표를 버린다", "값이
        있는 것만" 같은 규칙이 코드가 아니라 셀렉터에 적힌다(soupsieve 가
        ``:has()``·``:not()``·``:empty`` 를 지원한다 -- 실측). 그 규칙을 코드에
        넣으면 다음 페이지가 ``dd`` 나 ``span`` 을 쓸 때 또 분기가 생긴다.

        ``label`` 은 선택이고, 블록 **안에서** 한 요소를 찾아 그 텍스트를
        ``label`` 로 남긴다. 번호 대신 이름으로 블록을 집기 위한 것이다:
        셀렉터가 빈 블록을 걸러내면 ``block_index`` 가 문서마다 달라진다(이
        채권은 주식관련사채·유동화 섹션이 비어 빠지고, 그게 있는 채권은
        남는다). 번호를 안정적으로 두려면 걸러내지 말고 전부 담아야 하는데
        그쪽은 빈 섹션 마크업을 같이 저장한다(실측: 12블록 10,836B vs
        16블록 15,364B). 어느 쪽이든 ``label`` 이 있으면 이름으로 찾는다.

        ``block_index`` 는 **버리기 전에** 매긴다. 거른 뒤에 매기면 그 번호가
        무엇을 가리키는지가 문서마다 달라진다.
        """
        spec = self.api.parse_json or {}
        selector = spec.get("blocks")
        if not selector:
            raise MappingError(f"{self.api.api_id}: parse_json needs 'blocks' "
                               f"(a css selector for one block) for response_type='html_block'")
        from bs4 import BeautifulSoup

        label_selector = spec.get("label")
        soup = BeautifulSoup(response.text, "html.parser")
        records: list[dict[str, Any]] = []
        for index, element in enumerate(soup.select(selector)):
            record: dict[str, Any] = {"block_index": index, "html": str(element)}
            if label_selector:
                found = element.select_one(label_selector)
                record["label"] = found.get_text(" ", strip=True) if found else None
            records.append(record)
        logger.info("{}: {} block(s) from {!r}", self.api.api_id, len(records), selector)
        return records

    def _parse_spreadsheet(self, response: httpx.Response) -> list[dict[str, Any]]:
        """response_type='xlsx'/'xls': a real spreadsheet file.

        Two formats because the managers use both -- NH and TIME send OOXML,
        Kiwoom still sends the OLE2 one that openpyxl refuses. Everything
        after reading the cells is shared with the other tabular sources.

        response_parse_json adds "sheet" (index, default 0). Cells come back
        as text so a code with leading zeros stays a code rather than
        becoming a number the sheet happened to store.
        """
        spec = self.api.parse_json or {}
        sheet_no = int(spec.get("sheet", 0))
        cells: list[list[str]]
        if self.api.response_type == "xlsx":
            import openpyxl

            book = openpyxl.load_workbook(io.BytesIO(response.content), read_only=True,
                                          data_only=True)
            sheet = book.worksheets[sheet_no]
            cells = [["" if c is None else str(c) for c in row]
                     for row in sheet.iter_rows(values_only=True)]
            book.close()
        else:
            import xlrd

            book = xlrd.open_workbook(file_contents=response.content)
            sheet = book.sheet_by_index(sheet_no)
            cells = [["" if c is None else str(c) for c in sheet.row_values(i)]
                     for i in range(sheet.nrows)]
        return rows_to_records([r for r in cells if any(str(x).strip() for x in r)], spec)

    def _apply_merge_fields(self, data: Any, per_selector: dict[str, list[dict[str, Any]]]) -> None:
        """Mutates ``per_selector`` in place: for each ``{selector: {source_dot_path:
        target_field}}`` entry in ``ApiMst.merge_fields_json``, resolves
        ``source_dot_path`` against the *whole* response (not scoped to that
        selector) and stamps it onto every one of that selector's records --
        e.g. pulling a KOSPI200 value from KIS's ``output3`` into each
        ``output1`` record, so it's guaranteed to be from the same response/
        timestamp rather than relying on a join across separately-saved rows."""
        for selector, field_map in (self.api.merge_fields_json or {}).items():
            records = per_selector.get(selector)
            if not records:
                continue
            injected = {target: _extract_json_scalar(data, source) for source, target in field_map.items()}
            for record in records:
                record.update(injected)

    def _persist_env_fields(self, data: Any) -> None:
        """For each ``{source_dot_path: ENV_VAR_NAME}`` entry in
        ``ApiMst.persist_env_json``, extract the value from the JSON response
        and persist it to ``.env``/``os.environ`` (see
        ``app.auth_config.persist_env_var``) -- e.g. a KIS OAuth
        token-refresh job persisting the fresh ``access_token`` as
        ``KIS_ACCESS_TOKEN``. Since every job runs as its own systemd-timer
        process and ``app.auth_config`` reloads ``.env`` at import time,
        every *later* job's ``${KIS_ACCESS_TOKEN}`` placeholder picks up the
        fresh value automatically -- no extra wiring needed, just schedule
        the refresh job earlier than the jobs that need the token."""
        for source, env_var in (self.api.persist_env_json or {}).items():
            value = _extract_json_scalar(data, source)
            if value is not None:
                persist_env_var(env_var, str(value))
                logger.info("{}: persisted {} to .env", self.api.api_id, env_var)

    # ---- collection -------------------------------------------------------

    def _collect_once(self, session=None) -> dict[str, list[dict[str, Any]]]:
        """Fetch (paginating automatically if the ApiMst row says there is
        more to ask for) and extract every selector. Saving what comes back,
        and retrying this through a fresh login when the site logged us out,
        are app.scrapers.base.BaseScraper's job."""
        if (self.api.response_type or "").lower() == "session":
            return self._log_in()
        if self.is_paginated():
            return self.fetch_all_pages()
        return self.parse(self.fetch())

    def _log_in(self) -> dict[str, list[dict[str, Any]]]:
        """response_type='session': make the request and keep only the cookies.

        The cookies land in the process-wide httpx.Client (see
        get_http_client), which is what every later row in the same run then
        sends -- the HTTP counterpart to a browser row's storage-state file.
        Being process-wide is also the limit: a login row only helps rows that
        run in the *same* process, which is why run_cycle puts session rows
        first (see app.services.execution.run_cycle) and why an expiry
        mid-batch is handled by logging in again rather than by ordering.

        A login that fails is worth catching here rather than three rows
        later: ``response_parse_json['expect']`` is a string the reply must
        contain (e.g. KRX answers a good login with ``"_error_code":"CD001"``
        and a bad one with a different code, both as HTTP 200), so a wrong
        password fails the login job instead of quietly failing every job
        after it."""
        response = self.fetch()
        expect = (self.api.session_json or {}).get("expect")
        if expect and expect not in response.text:
            raise ValueError(f"{self.api.api_id}: login reply does not contain "
                             f"{expect!r} -- {response.text[:200]}")
        logger.info("{}: logged in, session held by the shared client", self.api.api_id)
        return {}
