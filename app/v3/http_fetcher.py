"""Self-contained HTTP fetcher & format parser driven purely by ``ApiSpec``.

Zero dependency on Oracle, SQLModel, loguru, or tenacity:
- Uses ``httpx.Client`` with ``app/certs/*.pem`` added to the trust store.
- Reads per-host pacing intervals from Neon's ``host_policy`` snapshot
  (longest-suffix match), with zero hardcoded domain tables.
- Applies single-pass ``:KEY`` substitution and restricted ``${VAR}`` secret
  resolution (blocking infrastructure env vars).
- Supports ``page``, ``cursor``, and ``token`` pagination + seam dedup, and
  all tabular/document formats (JSON, XML, CSV/delimited, ZIP delimited,
  HTML table, HTML block, XLSX, XLS).
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

import httpx

from app.errors import ConfigError, MappingError, ParamError, SessionExpired, SourceRefused, classify
from app.v3.session_store import get_session_token
from app.v3.spec import ApiSpec, PagePagination

_PLACEHOLDER_RE = re.compile(r"(?<!\w):(\w+)(?!\w)")
_PAGE_MARKER_RE = re.compile(r"(?<!\w):PAGE(?!\w)")
_ENV_VAR_RE = re.compile(r"\$\{(\w+)\}")
_BLOCKED_ENV_PREFIXES = (
    "ORACLE_", "OCI_", "S3_", "NEON_", "PG", "POSTGRES_", "AWS_", "ADMIN_",
)
_BLOCKED_ENV_NAMES = frozenset({"DATABASE_URL", "SECRET_KEY", "PATH", "HOME", "USER"})
_EXTRA_CA_DIR = Path(__file__).resolve().parent.parent / "certs"

_client: httpx.Client | None = None
_client_lock = threading.Lock()
_pace_lock = threading.Lock()
_next_slot_at: dict[str, float] = {}


def _ssl_context() -> ssl.SSLContext:
    try:
        import certifi
        ctx = ssl.create_default_context(cafile=certifi.where())
    except ImportError:
        ctx = ssl.create_default_context()
    if _EXTRA_CA_DIR.exists():
        for pem in sorted(_EXTRA_CA_DIR.glob("*.pem")):
            ctx.load_verify_locations(cafile=str(pem))
    return ctx


def get_http_client() -> httpx.Client:
    global _client
    with _client_lock:
        if _client is None or _client.is_closed:
            _client = httpx.Client(timeout=30.0, follow_redirects=True, verify=_ssl_context())
            atexit.register(close_http_client)
        return _client


def close_http_client() -> None:
    global _client
    with _client_lock:
        if _client is not None and not _client.is_closed:
            _client.close()
        _client = None


def substitute_placeholders(text: str, params: dict[str, Any]) -> str:
    if not params or ":" not in text:
        return text
    return _PLACEHOLDER_RE.sub(
        lambda m: str(params[m.group(1)]) if m.group(1) in params else m.group(0),
        text,
    )


def resolve_secret_placeholders(text: str) -> str:
    def _rep(match: re.Match[str]) -> str:
        name = match.group(1)
        upper = name.upper()
        if upper in _BLOCKED_ENV_NAMES or upper.startswith(_BLOCKED_ENV_PREFIXES):
            raise PermissionError(f"restricted env var {name!r} cannot be referenced in ApiSpec")
        token = get_session_token(name)
        if token is not None:
            return token
        return os.environ.get(name, match.group(0))

    return _ENV_VAR_RE.sub(_rep, text)


def _resolve_text(text: str, params: dict[str, Any]) -> str:
    return resolve_secret_placeholders(substitute_placeholders(text, params))


def interval_for_host(
    host: str,
    host_policies: dict[str, dict[str, Any]] | None = None,
    api_min: float | None = None,
    default_interval: float = 0.15,
) -> float:
    interval = default_interval
    if host_policies:
        for suffix, policy in sorted(host_policies.items(), key=lambda kv: len(kv[0]), reverse=True):
            if host == suffix or host.endswith("." + suffix):
                interval = float(policy.get("min_interval_sec", default_interval))
                break
    if api_min is not None:
        interval = max(interval, float(api_min))
    return interval


def pace_host(
    url: str,
    host_policies: dict[str, dict[str, Any]] | None = None,
    api_min: float | None = None,
) -> None:
    host = httpx.URL(url).host or ""
    interval = interval_for_host(host, host_policies=host_policies, api_min=api_min)
    if interval <= 0:
        return
    with _pace_lock:
        now = time.monotonic()
        slot = max(now, _next_slot_at.get(host, 0.0))
        _next_slot_at[host] = slot + interval
    sleep_for = slot - time.monotonic()
    if sleep_for > 0:
        time.sleep(sleep_for)


def _extract_json_records(data: Any, selector: str) -> list[dict[str, Any]]:
    if selector == ".":
        if isinstance(data, list):
            return [r for r in data if isinstance(r, dict)]
        return [data] if isinstance(data, dict) else []
    cur = data
    for part in selector.split("."):
        if not isinstance(cur, dict):
            return []
        cur = cur.get(part)
    if cur is None:
        return []
    if isinstance(cur, dict):
        return [cur]
    if isinstance(cur, list):
        return [r for r in cur if isinstance(r, dict)]
    return []


def _extract_json_scalar(data: Any, path: str) -> Any:
    cur = data
    for part in path.split("."):
        if not isinstance(cur, dict):
            return None
        cur = cur.get(part)
    return cur


def _extract_path(data: Any, path: str) -> list[Any]:
    current: list[Any] = [data]
    for seg in path.split("."):
        explode = seg.endswith("[]")
        key = seg[:-2] if explode else seg
        nxt: list[Any] = []
        for item in current:
            if not isinstance(item, dict):
                continue
            val = item.get(key)
            if val is None:
                continue
            nxt.extend(val if explode and isinstance(val, list) else [val])
        current = nxt
    return current


def _rows_to_records(rows: list[list[str]], spec: dict[str, Any]) -> list[dict[str, Any]]:
    skip = int(spec.get("skip_rows", 0))
    if skip:
        rows = rows[skip:]
    has_header = bool(spec.get("has_header", False))
    fields = spec.get("fields")
    if has_header and rows:
        header, rows = rows[0], rows[1:]
        fields = fields or [h.strip() for h in header]
    if not fields:
        raise MappingError("parse spec needs 'fields' or 'has_header': true")
    return [{name: (v.strip() or None) for name, v in zip(fields, row)} for row in rows]


def _extract_xml_records(root: ET.Element, selector: str) -> list[dict[str, Any]]:
    path, _, preferred_attr = selector.partition("#") if "#" in selector else (selector, "", None)
    xpath = path if (path.startswith(".") or "/" in path) else f".//{path.replace('.', '/')}"

    def _child_val(el: ET.Element) -> Any:
        if list(el):
            return _group(el)
        if preferred_attr and preferred_attr in el.attrib:
            return el.get(preferred_attr)
        if "value" in el.attrib:
            return el.get("value")
        if len(el.attrib) == 1:
            return next(iter(el.attrib.values()))
        if len(el.attrib) > 1:
            return dict(el.attrib)
        return el.text.strip() if el.text and el.text.strip() else None

    def _group(el: ET.Element) -> dict[str, Any]:
        grouped: dict[str, list[Any]] = {}
        for child in el:
            grouped.setdefault(child.tag, []).append(_child_val(child))
        return {k: (v[0] if len(v) == 1 else v) for k, v in grouped.items()}

    out: list[dict[str, Any]] = []
    for el in root.findall(xpath):
        out.append(_group(el) if list(el) else dict(el.attrib))
    return out


def parse_response(response: httpx.Response, spec: ApiSpec) -> list[dict[str, Any]]:
    ext = spec.engine.extract
    rtype = ext.response_type.lower()
    parse_cfg = ext.parse or {}

    if rtype == "html_table":
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(response.text, "html.parser")
        tables = soup.find_all("table")
        if not tables:
            return []
        table = tables[int(parse_cfg.get("table_index", 0))]
        rows = [[c.get_text(" ", strip=True) for c in tr.find_all(["td", "th"])] for tr in table.find_all("tr")]
        return _rows_to_records([r for r in rows if any(r)], parse_cfg)

    if rtype == "html_block":
        from bs4 import BeautifulSoup
        sel = parse_cfg.get("blocks")
        if not sel:
            raise MappingError("parse.blocks is required for response_type='html_block'")
        soup = BeautifulSoup(response.text, "html.parser")
        label_sel = parse_cfg.get("label")
        out: list[dict[str, Any]] = []
        for idx, el in enumerate(soup.select(sel)):
            rec: dict[str, Any] = {"block_index": idx, "html": str(el)}
            if label_sel:
                found = el.select_one(label_sel)
                rec["label"] = found.get_text(" ", strip=True) if found else None
            out.append(rec)
        return out

    if rtype in ("xlsx", "xls"):
        sheet_no = int(parse_cfg.get("sheet", 0))
        if rtype == "xlsx":
            import openpyxl
            book = openpyxl.load_workbook(io.BytesIO(response.content), read_only=True, data_only=True)
            try:
                sheet = book.worksheets[sheet_no]
                cells = [["" if c is None else str(c) for c in row] for row in sheet.iter_rows(values_only=True)]
            finally:
                book.close()
        else:
            import xlrd
            book = xlrd.open_workbook(file_contents=response.content)
            sheet = book.sheet_by_index(sheet_no)
            cells = [["" if c is None else str(c) for c in sheet.row_values(i)] for i in range(sheet.nrows)]
        return _rows_to_records([r for r in cells if any(str(x).strip() for x in r)], parse_cfg)

    if rtype in ("zip_delimited", "delimited"):
        encoding = parse_cfg.get("encoding", "utf-8")
        if rtype == "zip_delimited":
            with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
                names = zf.namelist()
                if not names:
                    return []
                inner = parse_cfg.get("inner_file") or names[0]
                text = zf.read(inner).decode(encoding)
        else:
            text = response.content.decode(encoding)
        delimiter = parse_cfg.get("delimiter", ",")
        rows = [r for r in csv.reader(io.StringIO(text), delimiter=delimiter) if r]
        return _rows_to_records(rows, parse_cfg)

    content_type = response.headers.get("content-type", "")
    is_xml = rtype == "xml" or (rtype == "auto" and ("xml" in content_type or response.text.lstrip().startswith("<")))
    if is_xml:
        root = ET.fromstring(response.text)
        return _extract_xml_records(root, ext.selector)

    data = response.json()
    records = _extract_json_records(data, ext.selector)
    if ext.merge_fields and records:
        injected = {target: _extract_json_scalar(data, src) for src, target in ext.merge_fields.items()}
        for rec in records:
            rec.update(injected)
    return records


def build_http_request(spec: ApiSpec, params: dict[str, Any]) -> dict[str, Any]:
    req = spec.engine.request
    headers = {
        k: (_resolve_text(v, params) if isinstance(v, str) else v)
        for k, v in req.headers.items()
    }
    kwargs: dict[str, Any] = {
        "method": (req.method or "GET").upper(),
        "url": _resolve_text(req.url, params),
        "headers": headers,
    }
    if req.payload_type == "content":
        kwargs["content"] = _resolve_text(req.payload_xml or "", params).encode("utf-8")
    elif req.payload_type in ("data", "json"):
        merged = {**(req.payload_json or {}), **params}
        kwargs[req.payload_type] = {
            k: (resolve_secret_placeholders(v) if isinstance(v, str) else v)
            for k, v in merged.items()
        }
    elif req.payload_type is not None:
        raise ParamError(f"unsupported payload_type={req.payload_type!r}")

    if spec.engine.pacing.timeout_sec is not None:
        kwargs["timeout"] = spec.engine.pacing.timeout_sec
    return kwargs


def _check_markers(response: httpx.Response, spec: ApiSpec, api_id: str) -> None:
    sess = spec.engine.session
    if not sess.logged_out and not sess.blocked:
        return
    head = response.content[:4]
    body = "" if head[:2] == b"PK" or head == b"%PDF" else response.text[:4000]
    if sess.logged_out and sess.logged_out in body:
        raise SessionExpired(f"{api_id}: reply matched logged_out marker {sess.logged_out!r}")
    if sess.blocked and sess.blocked in body:
        raise SourceRefused(
            f"{api_id}: reply matched blocked marker {sess.blocked!r}",
            host=httpx.URL(str(response.url)).host or "",
        )


def fetch_once(
    api_id: str,
    spec: ApiSpec,
    params: dict[str, Any],
    *,
    host_policies: dict[str, dict[str, Any]] | None = None,
    max_attempts: int = 3,
) -> httpx.Response:
    last_exc: BaseException | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            kwargs = build_http_request(spec, params)
            pace_host(kwargs["url"], host_policies=host_policies, api_min=spec.engine.pacing.min_interval_sec)
            resp = get_http_client().request(**kwargs)
            _check_markers(resp, spec, api_id)
            resp.raise_for_status()
            return resp
        except Exception as exc:
            err = classify(exc)
            last_exc = err
            if not err.retryable or attempt >= max_attempts:
                raise err from exc
            time.sleep(min(10.0, float(2 ** (attempt - 1))))
    raise last_exc or ConfigError(f"{api_id}: fetch failed")


def _merge_page_records(
    accumulated: list[dict[str, Any]],
    seen: dict[str, int],
    page_records: list[dict[str, Any]],
    identity_field: str | None = None,
) -> None:
    for rec in page_records:
        if identity_field and identity_field in rec:
            ident = f"@{rec[identity_field]}"
        else:
            ident = json.dumps(rec, sort_keys=True, default=str)
        if ident in seen:
            accumulated[seen[ident]] = rec
        else:
            seen[ident] = len(accumulated)
            accumulated.append(rec)


def _effective_pagination(spec: ApiSpec):
    """Explicit ``engine.pagination`` wins; otherwise implicit ``:PAGE`` marker
    in url/payload means 1-based page pagination (matching v2)."""
    if spec.engine.pagination is not None:
        return spec.engine.pagination
    req = spec.engine.request
    template = (req.url or "") + (req.payload_xml or "")
    if _PAGE_MARKER_RE.search(template) or "PAGE" in (req.payload_json or {}):
        return PagePagination(mode="page", param="PAGE", start=1)
    return None


def parse_response_for_selector(
    response: httpx.Response,
    spec: ApiSpec,
    selector: str,
) -> list[dict[str, Any]]:
    """Extract records for one explicit JSON/XML selector from ``response``."""
    ext = spec.engine.extract
    rtype = ext.response_type.lower()
    content_type = response.headers.get("content-type", "")
    is_xml = rtype == "xml" or (rtype == "auto" and ("xml" in content_type or response.text.lstrip().startswith("<")))
    if is_xml:
        root = ET.fromstring(response.text)
        return _extract_xml_records(root, selector)
    data = response.json()
    records = _extract_json_records(data, selector)
    if ext.merge_fields and records:
        injected = {target: _extract_json_scalar(data, src) for src, target in ext.merge_fields.items()}
        for rec in records:
            rec.update(injected)
    return records


def collect_http_bundle(
    api_id: str,
    spec: ApiSpec,
    params: dict[str, Any],
    *,
    host_policies: dict[str, dict[str, Any]] | None = None,
) -> tuple[list[dict[str, Any]], dict[str, list[dict[str, Any]]]]:
    """Fetch (with optional pagination) primary records plus any ``extract.extra_selectors``.

    Returns ``(primary_records, {extra_target_name: extra_records})``.
    """
    work_params = dict(params)
    extra_map = spec.engine.extract.extra_selectors or {}
    extra_accum: dict[str, list[dict[str, Any]]] = {t: [] for t in extra_map.values()}
    extra_seen: dict[str, dict[str, int]] = {t: {} for t in extra_map.values()}

    def _capture_extras(resp: httpx.Response) -> None:
        for sel, target in extra_map.items():
            recs = parse_response_for_selector(resp, spec, sel)
            if recs:
                _merge_page_records(extra_accum[target], extra_seen[target], recs)

    pag = _effective_pagination(spec)
    if pag is None:
        resp = fetch_once(api_id, spec, work_params, host_policies=host_policies)
        primary = parse_response(resp, spec)
        _capture_extras(resp)
        return primary, extra_accum

    page_delay = spec.engine.pacing.page_delay_sec
    if page_delay is None:
        page_delay = float(os.environ.get("PAGE_DELAY_SEC", "0.3"))

    if pag.mode == "page":
        work_params[pag.param] = pag.start

    accumulated: list[dict[str, Any]] = []
    seen: dict[str, int] = {}
    previous: list[dict[str, Any]] | None = None
    cursor_field = pag.from_path.split(".")[-1] if pag.mode == "cursor" else None

    for _ in range(1, pag.max_pages + 1):
        resp = fetch_once(api_id, spec, work_params, host_policies=host_policies)
        page_recs = parse_response(resp, spec)
        if not page_recs or page_recs == previous:
            break
        _merge_page_records(accumulated, seen, page_recs, cursor_field)
        _capture_extras(resp)
        previous = page_recs

        # Advance continuation cursor/page/token
        if pag.mode == "page":
            work_params[pag.param] = int(work_params.get(pag.param, pag.start)) + 1
        elif pag.mode == "cursor":
            if pag.min_records is not None and len(page_recs) < pag.min_records:
                break
            try:
                body = resp.json()
            except Exception:
                body = {}
            vals = _extract_path(body, pag.from_path)
            if not vals:
                break
            nxt = min(vals) if pag.pick == "min" else max(vals)
            if work_params.get(pag.param) == nxt:
                break
            work_params[pag.param] = nxt
        elif pag.mode == "token":
            try:
                body = resp.json()
            except Exception:
                body = {}
            if pag.continue_when:
                src = pag.continue_when["source"]
                actual = resp.headers.get(src[7:]) if src.startswith("header:") else (_extract_path(body, src) or [None])[0]
                if actual not in pag.continue_when.get("in", []):
                    break
            ok = True
            for p_name, src in pag.params.items():
                val = resp.headers.get(src[7:]) if src.startswith("header:") else (_extract_path(body, src) or [None])[0]
                if val is None:
                    ok = False
                    break
                work_params[p_name] = val
            if not ok:
                break
        else:
            break

        if page_delay > 0:
            time.sleep(page_delay)

    return accumulated, extra_accum


def collect_http_records(
    api_id: str,
    spec: ApiSpec,
    params: dict[str, Any],
    *,
    host_policies: dict[str, dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """Fetch (with optional pagination) and parse primary raw records for one HTTP ``ApiSpec``."""
    primary, _ = collect_http_bundle(api_id, spec, params, host_policies=host_policies)
    return primary
