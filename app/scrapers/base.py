"""Everything a scraper does that has nothing to do with how it fetched.

:class:`BaseScraper` holds the half of the engine that is identical whether a
result arrived over httpx (:mod:`app.scrapers.dynamic`) or was acted out in a
real browser (:mod:`app.scrapers.browser`): ``:KEY`` substitution, staging a
downloaded document, merging pages of records, and saving the finished
``{selector: [record, ...]}`` into whatever tables ``output_tables_json``
names.

A subclass supplies exactly one thing -- :meth:`collect`, "hand me
``{selector: [record, ...]}``". Everything after that point is shared, so a
second transport cannot quietly grow its own idea of what a saved row looks
like, and a result table cannot tell which transport produced it.
"""

from __future__ import annotations

import io
import json
import re
import zipfile
from typing import Any

from loguru import logger
from sqlmodel import Session

from app.db.models import ApiMst
from app.services.export import TABLE_REGISTRY, append_csv_rows, stage_file


def merge_records(
    all_records: dict[str, list[dict[str, Any]]],
    seen: dict[str, dict[str, int]],
    per_selector: dict[str, list[dict[str, Any]]],
    identity_field: str | None = None,
) -> None:
    """Fold one page's records into the accumulated set, the later page
    winning at the seam.

    A page boundary can hand back a record that was already seen, and it can
    come back *different*: the first reply saw it truncated by a record cap
    and the next sees it whole -- observed as a bar reappearing with a larger
    volume or a lower low. Keying on ``identity_field`` (when the continuation
    is anchored on one) rather than on the whole record means the fuller
    version replaces the clipped one instead of both being kept as distinct.
    """
    for selector, records in per_selector.items():
        bucket = all_records.setdefault(selector, [])
        index = seen.setdefault(selector, {})
        for record in records:
            if identity_field and identity_field in record:
                identity = f"@{record[identity_field]}"
            else:
                identity = json.dumps(record, sort_keys=True, default=str)
            if identity in index:
                bucket[index[identity]] = record
                continue
            index[identity] = len(bucket)
            bucket.append(record)


def rows_to_records(rows: list[list[str]], spec: dict[str, Any]) -> list[dict[str, Any]]:
    """A rectangle of cells -> records, shared by every tabular source.

    Delimited text, an HTML table and a spreadsheet all arrive as the
    same thing once read: a header somewhere near the top and rows under
    it. Only the reading differs, so only the reading is per-format.

    response_parse_json keys (see
    app.scrapers.dynamic._parse_delimited_text for the delimited-only ones):
    - "skip_rows": drop this many rows before anything else. Sheets and
      exported tables often open with a title or a note above the header
      (TIGER's holdings table starts with a lone '- 설정/해지현황' cell).
    - "has_header": the first remaining row is the header -- skipped, and
      used as the field names when "fields" is not given.
    - "fields": explicit column names, in order. Needed whenever the
      header is prose rather than identifiers, which is the usual case
      for a table meant to be read by a person: '수량(주)' cannot be a
      column name, and translating it in code would put per-site
      knowledge where this engine keeps none.
    """
    skip = int(spec.get("skip_rows", 0))
    if skip:
        rows = rows[skip:]
    has_header = spec.get("has_header", False)
    fields = spec.get("fields")
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


class SessionExpired(RuntimeError):
    """The site answered as if nobody were logged in.

    Raised by whichever transport noticed -- a browser row seeing its
    ``logged_out`` marker on the page, an HTTP row seeing that marker in a
    reply body -- so :meth:`BaseScraper.collect` can log in again and retry
    once instead of failing the job."""


class BaseScraper:
    """One ``ApiMst`` row, minus any opinion about how it is fetched.

    ``params`` values are substituted as ``:KEY`` placeholders (Oracle
    bind-variable style, e.g. ``:KACD``, matched as a whole token) wherever a
    subclass runs a string through :meth:`_substitute_placeholders` -- a url,
    a header, a request body, a value typed into a form field.
    """

    def __init__(self, api: ApiMst, params: dict[str, Any] | None = None):
        self.api = api
        self.params = params or {}

    # ---- shared request building -----------------------------------------

    def _substitute_placeholders(self, text: str) -> str:
        for key, value in self.params.items():
            # ':KEY' (Oracle bind-variable style), matched as a whole token so
            # ':KACD' doesn't also swallow something like ':KACD2'.
            text = re.sub(rf"(?<!\w):{re.escape(key)}(?!\w)", str(value), text)
        return text

    # ---- collection (the one thing a subclass owns) ----------------------

    def collect(self, session: Session | None = None) -> dict[str, list[dict[str, Any]]]:
        """Produce ``{selector: [record, ...]}``, logging in again if the site
        turns out to have logged us out mid-batch.

        A logged-in session is short (KRX's is 30 minutes) and a batch is not,
        so expiry lands in the middle of a run rather than at its start --
        which is why ordering the login first is an optimisation and this is
        the actual mechanism. The retry is deliberately once: a wrong password
        must not turn into a login loop that locks the account.

        Only a row that names its login row (``response_parse_json['login']``,
        an api_id) can do this, and only when it was handed a DB session to
        look that row up with -- which is why ``session`` is passed down from
        :meth:`run_and_save` at all."""
        can_relogin = (bool(self._login_api_id) and session is not None
                       and (self.api.response_type or "").lower() != "session")
        for attempt in (1, 2):
            try:
                return self._collect_once(session)
            except SessionExpired as exc:
                if attempt == 2 or not can_relogin:
                    raise
                logger.warning("{} -- logging in again and retrying once", exc)
                self._login(session)
        raise AssertionError("unreachable")  # pragma: no cover

    def _collect_once(self, session: Session | None) -> dict[str, list[dict[str, Any]]]:
        """One attempt at collecting -- the whole of what a transport owns."""
        raise NotImplementedError

    # ---- logging back in --------------------------------------------------

    @property
    def _login_api_id(self) -> str | None:
        """The api_id of the row that logs this site in, if this row says.

        Named by the data row rather than discovered, so a login that lives on
        a different host than the screens it unlocks (an SSO page) still
        works, and so no job has to scan the master table to find out who its
        login is."""
        return (self.api.response_parse_json or {}).get("login")

    def _invalidate_session(self) -> None:
        """Throw away whatever stale session this transport is holding, before
        logging in again. A browser row has a storage-state file to delete; an
        HTTP row's cookie jar is replaced by the login reply itself."""

    def _login(self, session: Session) -> None:
        """Run this row's login row, replacing the stale session.

        The login row is an ordinary ``ApiMst`` row with
        ``response_type='session'`` -- so credentials, steps and pacing all
        stay where every other row's do, and logging in is just another row
        being run. It goes through :func:`app.scrapers.make_scraper` like
        anything else, so an HTTP row's login may be a browser row and vice
        versa if a site ever needs that."""
        from app.scrapers import make_scraper

        login_api: ApiMst | None = session.get(ApiMst, self._login_api_id)
        if login_api is None:
            raise ValueError(f"{self.api.api_id}: no ApiMst row for "
                             f"login api_id={self._login_api_id!r}")
        self._invalidate_session()
        make_scraper(login_api, params=self.params).collect(session)

    # ---- staging a document ----------------------------------------------

    def _stage_binary(self, content: bytes, content_type: str | None = None,
                      fallback_name: str | None = None) -> list[dict[str, Any]]:
        """Save a downloaded document and describe it, one record per file.

        ``response_parse_json`` supplies the layout:
        {"group": "dart_docs", "name": ":RCEPT_NO.pdf"} -- ``name`` goes
        through the same ':KEY' substitution as the url and payload, so a
        document is named from the job's own parameters rather than from
        whatever the server happened to call it. Falling back to the job's
        params keeps a misconfigured row from silently overwriting one file
        over and over.

        The returned record is metadata only; the bytes are already on disk.
        Deliberately not the file content -- a result table is the wrong place
        for a multi-megabyte blob, and the object store is where the pipeline
        already puts large artifacts."""
        config = self.api.response_parse_json or {}
        group = config.get("group") or self.api.api_id.lower()
        name = self._substitute_placeholders(config.get("name") or "")
        if not name or ":" in name:
            # No usable template: fall back to something unique per job rather
            # than a fixed name every run would clobber. ``fallback_name`` is
            # what the source itself called the file, which only a browser
            # download actually knows (an HTTP row has nothing but headers) --
            # still second to the configured template, since a site is free to
            # send every job the same filename.
            if fallback_name:
                name = fallback_name
            else:
                # "suffix" is what gives the joined params an extension; a
                # name the source supplied already has one.
                joined = "_".join(str(v) for v in self.params.values()) or self.api.api_id
                name = f"{joined}{config.get('suffix', '')}"
        # "unzip": true stores the archive's contents instead of the archive.
        # Worth it when the payload is a single document that is more useful
        # readable than packed -- DART wraps one XML per disclosure, and an
        # unpacked XML can be read straight from the bucket. Costs space
        # (that XML is ~8x its zipped size), so it stays opt-in.
        if config.get("unzip") and content[:2] == b"PK":
            records = []
            with zipfile.ZipFile(io.BytesIO(content)) as archive:
                for info in archive.infolist():
                    if info.is_dir():
                        continue
                    body = archive.read(info)
                    stage_file(group, info.filename, body)
                    records.append(self._describe(group, info.filename, len(body), None))
            logger.info("{}: staged {} file(s) from archive {}",
                        self.api.api_id, len(records), name)
            return records

        path = stage_file(group, name, content)
        logger.info("{}: staged {} ({} bytes)", self.api.api_id, path, len(content))
        return [self._describe(group, name, len(content), content_type)]

    @staticmethod
    def _describe(group: str, name: str, size: int, content_type: str | None) -> dict[str, Any]:
        """Metadata record for one staged file -- what the result table gets in
        place of the bytes, and enough to locate the object afterwards."""
        return {
            "group": group,
            "file_name": name,
            "object_key": f"{group}/{name}",
            "byte_size": size,
            "content_type": content_type,
        }

    # ---- persistence ------------------------------------------------------

    def run_and_save(
        self,
        session: Session,
        job_id: str,
        key_params: dict[str, Any] | None = None,
    ) -> dict[str, int]:
        """Collect every selector and save each to its mapped table. Returns
        ``{table_name: record_count}`` (summed if >1 selector maps to the same
        table).

        ``key_params`` (the generating job's ``ApiMst.key_params_list``
        subset, see :func:`app.services.execution.run_job`) is stamped onto every
        saved row: as real columns for a dedicated/typed table (when its
        field names match), or as ``key_params_json`` for a generic table --
        so results are filterable by e.g. instrument/date without parsing
        job_id or joining back to ApiJob.params_json."""
        mapping = self.api.output_tables_json or {}
        key_params = key_params or {}

        per_selector = self.collect(session)

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

            # Buffered only for dedicated (non-generic) tables -- api_rst's
            # free-form result_json has no fixed schema to write as columns.
            # This is a local trail of what the run produced; the bucket is
            # fed from the tables themselves (scripts/sql/sp_export_parquet).
            if typed_rows:
                append_csv_rows(table_name, job_id, typed_rows)

        # Deliberately no commit here -- the caller owns the transaction (see
        # app.services.execution.run_job, which folds these rows, the ApiJob
        # update and the ApiJobLog entry into a single commit). Each commit is
        # a full round-trip to Oracle ADB, measured at ~32ms; committing here
        # as well made three per job, which at hundreds of jobs per cycle cost
        # more than the HTTP requests themselves (~24ms each).

        for table_name, count in counts.items():
            logger.info("{}: saved {} record(s) into {}", self.api.api_id, count, table_name)
        return counts
