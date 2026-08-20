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

import csv
import io
import os
import re
import time
import xml.etree.ElementTree as ET
import zipfile
from typing import Any

import httpx
from loguru import logger
from sqlmodel import Session
from tenacity import retry, stop_after_attempt, wait_exponential

from app.auth_config import persist_env_var, resolve_env_placeholders
from app.db.models import ApiMst
from app.services.export import TABLE_REGISTRY, append_csv_rows

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
    single-record list; missing/non-dict intermediate steps yield []."""
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


class DynamicApiScraper:
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
    pagination: :meth:`run_and_save` then calls :meth:`fetch_all_pages`
    instead of a single :meth:`fetch`, incrementing ``:PAGE`` from 1 and
    stopping once a page comes back with no records for any selector.
    """

    def __init__(self, api: ApiMst, params: dict[str, Any] | None = None):
        self.api = api
        self.params = params or {}

    # ---- request building --------------------------------------------------

    def _substitute_placeholders(self, text: str) -> str:
        for key, value in self.params.items():
            # ':KEY' (Oracle bind-variable style), matched as a whole token so
            # ':KACD' doesn't also swallow something like ':KACD2'.
            text = re.sub(rf"(?<!\w):{re.escape(key)}(?!\w)", str(value), text)
        return text

    def build_request(self) -> dict[str, Any]:
        api = self.api
        headers = {
            k: (resolve_env_placeholders(self._substitute_placeholders(v)) if isinstance(v, str) else v)
            for k, v in (api.header_json or {}).items()
        }
        kwargs: dict[str, Any] = {
            "method": api.request_type.upper(),
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
            raise ValueError(f"{api.api_id}: payload_type must be one of {valid}, got '{api.payload_type}'")

        return kwargs

    # ---- execution ------------------------------------------------------

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=10))
    def fetch(self) -> httpx.Response:
        request_kwargs = self.build_request()
        with httpx.Client(timeout=30, follow_redirects=True) as client:
            response = client.request(**request_kwargs)
            response.raise_for_status()
            return response

    def is_paginated(self) -> bool:
        # api_url/payload_xml use :TOKEN string substitution, so a literal
        # ':PAGE' anywhere in them is the marker. payload_json instead uses
        # key-overlay substitution (see build_request) -- a ':PAGE' *value*
        # there wouldn't actually get substituted (only a key literally
        # named 'PAGE' gets overlaid by self.params['PAGE'] during
        # fetch_all_pages), so a 'PAGE' *key* is that mechanism's own marker.
        template = (self.api.api_url or "") + (self.api.payload_xml or "")
        if re.search(rf"(?<!\w):{_PAGE_PARAM}(?!\w)", template):
            return True
        return _PAGE_PARAM in (self.api.payload_json or {})

    def fetch_all_pages(
        self,
        start_page: int = 1,
        max_pages: int = 100,
        delay: float | None = None,
    ) -> dict[str, list[dict[str, Any]]]:
        """Repeatedly fetch pages by incrementing ``params[:PAGE]``, merging
        every page's records per selector. Stops when:
        - a page comes back with zero records for every selector (most APIs
          on an out-of-range page), or
        - a page's content is identical to the immediately preceding page --
          some APIs (e.g. DART) clamp an out-of-range page_no to the last
          valid page instead of returning empty, so without this check the
          loop would otherwise run to ``max_pages`` re-saving the same last
          page's records over and over,
        - or ``max_pages`` is hit (safety net against a selector that never
          goes empty/repeats, e.g. one that's misconfigured).

        ``delay`` (seconds between page fetches) defaults to ``PAGE_DELAY_SEC``
        in ``.env`` (see ``_PAGE_DELAY_SEC``) when not explicitly passed;
        pass it explicitly only to override that for a single call."""
        delay = _PAGE_DELAY_SEC if delay is None else delay
        all_records: dict[str, list[dict[str, Any]]] = {}
        previous_page: dict[str, list[dict[str, Any]]] | None = None

        for page in range(start_page, start_page + max_pages):
            self.params[_PAGE_PARAM] = page
            response = self.fetch()
            per_selector = self.parse(response)

            count = sum(len(r) for r in per_selector.values())
            logger.info("{}: page {} -> {} record(s)", self.api.api_id, page, count)

            if count == 0:
                break
            if per_selector == previous_page:
                logger.info("{}: page {} repeats the previous page, stopping", self.api.api_id, page)
                break

            for selector, records in per_selector.items():
                all_records.setdefault(selector, []).extend(records)
            previous_page = per_selector

            time.sleep(delay)

        return all_records

    # ---- response parsing -----------------------------------------------

    def parse(self, response: httpx.Response) -> dict[str, list[dict[str, Any]]]:
        """Run every selector in ``output_tables_json`` against the response,
        returning ``{selector: [record, ...]}``. For JSON responses,
        ``merge_fields_json`` (if set) then injects extra fields sourced from
        elsewhere in the same response into those records, and
        ``persist_env_json`` (if set) persists fields to ``.env`` -- see
        ``ApiMst.merge_fields_json``/``ApiMst.persist_env_json`` and
        :meth:`_apply_merge_fields`/:meth:`_persist_env_fields`."""
        mapping = self.api.output_tables_json or {}
        if not mapping and not self.api.persist_env_json:
            raise ValueError(f"{self.api.api_id}: output_tables_json is empty")

        if self.api.response_type in ("zip_delimited", "delimited"):
            # Binary/plain-text response, not XML/JSON -- response.text may
            # be garbage (zip) or just isn't XML/JSON, so this must be
            # checked (and handled) before any content-type sniffing below.
            # No real "selector" concept applies to a flat delimited file
            # (unlike an XML/JSON path into a nested response) -- every key
            # in output_tables_json just gets the same full record list,
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
        spec = self.api.response_parse_json or {}
        encoding = spec.get("encoding", "utf-8")

        zf = zipfile.ZipFile(io.BytesIO(response.content))
        inner_file = spec.get("inner_file") or zf.namelist()[0]
        text = zf.read(inner_file).decode(encoding)
        return self._parse_delimited_text(text, spec)

    def _parse_delimited(self, response: httpx.Response) -> list[dict[str, Any]]:
        """response_type='delimited': the response body itself (no zip) is
        the delimited text -- a plain .csv/.txt download. Same
        response_parse_json shape as zip_delimited, minus 'inner_file'."""
        spec = self.api.response_parse_json or {}
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
        if has_header and rows:
            header, rows = rows[0], rows[1:]
            fields = fields or [h.strip() for h in header]

        if not fields:
            raise ValueError("response_parse_json needs 'fields' (or 'has_header': true)")

        records = []
        for row in rows:
            values = [v.strip() for v in row]
            records.append({name: (v or None) for name, v in zip(fields, values)})
        return records

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

    # ---- persistence ------------------------------------------------------

    def run_and_save(
        self,
        session: Session,
        job_id: str,
        key_params: dict[str, Any] | None = None,
    ) -> dict[str, int]:
        """Fetch (paginating automatically if the ApiMst row has a ``:PAGE``
        placeholder), extract every selector, and save each to its mapped
        table. Returns ``{table_name: record_count}`` (summed if >1 selector
        maps to the same table).

        ``key_params`` (the generating job's ``ApiMst.key_params_list``
        subset, see :func:`app.services.execution.run_job`) is stamped onto every
        saved row: as real columns for a dedicated/typed table (when its
        field names match), or as ``key_params_json`` for a generic table --
        so results are filterable by e.g. instrument/date without parsing
        job_id or joining back to ApiJob.params_json."""
        mapping = self.api.output_tables_json or {}
        key_params = key_params or {}

        if self.is_paginated():
            per_selector = self.fetch_all_pages()
        else:
            response = self.fetch()
            per_selector = self.parse(response)

        counts: dict[str, int] = {}
        for selector, table_name in mapping.items():
            model_cls = TABLE_REGISTRY.get(table_name)
            if model_cls is None:
                raise ValueError(f"{self.api.api_id}: unknown target table '{table_name}'")

            records = per_selector.get(selector, [])
            is_generic = "result_json" in model_cls.model_fields
            typed_rows: list[dict[str, Any]] = []
            for record in records:
                if is_generic:
                    obj = model_cls(
                        api_id=self.api.api_id,
                        job_id=job_id,
                        key_params_json=key_params or None,
                        result_json=record,
                    )
                else:
                    # model_validate (not plain __init__) so string values from
                    # JSON/XML actually get coerced to the field's real type
                    # (e.g. "19.20" -> 19.2 float) -- SQLModel's __init__
                    # doesn't validate, it just stores what it's given as-is.
                    # key_params first, record last, so a genuine field-name
                    # clash lets the API's own data win over the job param.
                    obj = model_cls.model_validate(
                        {"api_id": self.api.api_id, "job_id": job_id, **key_params, **record}
                    )
                    typed_rows.append(obj.model_dump(exclude={"id", "updated_at"}))
                session.add(obj)
            counts[table_name] = counts.get(table_name, 0) + len(records)

            # Buffered export applies only to dedicated (non-generic) tables --
            # api_rst's free-form result_json has no fixed schema to export.
            # See app.services.export.finalize_export for what turns this
            # into Parquet.
            if typed_rows:
                append_csv_rows(table_name, job_id, typed_rows)

        session.commit()

        for table_name, count in counts.items():
            logger.info("{}: saved {} record(s) into {}", self.api.api_id, count, table_name)
        return counts
