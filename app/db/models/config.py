"""엔진 부기 -- api_mst · api_job_builder · api_job · api_job_log · api_rst."""

import re
from datetime import date, datetime
from typing import Any, Optional

from pydantic import ConfigDict, model_validator
from sqlalchemy import CheckConstraint, Column, Date, DateTime, Identity, Integer, String, func
from sqlmodel import Field, SQLModel

from app.db.types import JSONText, OracleBoolean, XMLText
from app.db.models.base import ResultTable
from app.db.models.base import EtfPdfDateMixin, MdcRecordBase, VendorRecordBase




class ApiMst(SQLModel, table=True):
    __tablename__ = "api_mst"
    __table_args__ = (
        CheckConstraint(
            # payload_type names the httpx request kwarg to send the payload as
            # (see app.scrapers.dynamic.DynamicApiScraper.build_request):
            # NULL          -> no body at all (typical for GET),
            # 'json'/'data' -> dict-shaped body from payload_json,
            # 'content'     -> raw text body (XML, pre-built text, ...) from payload_xml.
            "(payload_type IS NULL AND payload_json IS NULL AND payload_xml IS NULL) OR "
            "(payload_type IN ('json', 'data') AND payload_json IS NOT NULL AND payload_xml IS NULL) OR "
            "(payload_type = 'content' AND payload_xml IS NOT NULL AND payload_json IS NULL)",
            name="ck_api_mst_payload_exclusive",
        ),
        CheckConstraint(
            # A browser row is driven by behavior_json (steps acted out in a
            # real browser, see app.scrapers.browser.BrowserScraper); an HTTP
            # row is driven by a payload. Neither shape can borrow the other's
            # column by accident, and request_type stays the single thing that
            # decides which engine a row goes to (see app.scrapers.make_scraper).
            "(UPPER(request_type) = 'BROWSER' AND behavior_json IS NOT NULL AND payload_type IS NULL) OR "
            "(UPPER(request_type) <> 'BROWSER' AND behavior_json IS NULL)",
            name="ck_api_mst_behavior_browser",
        ),
    )

    api_id: str = Field(primary_key=True, max_length=150)
    api_name: str = Field(max_length=100)
    api_group: str = Field(max_length=100)
    request_type: str = Field(max_length=20)
    api_url: str = Field(max_length=500)
    header_json: dict = Field(sa_type=JSONText)
    payload_type: Optional[str] = Field(default=None, max_length=20)
    payload_json: Optional[dict] = Field(default=None, sa_type=JSONText)
    payload_xml: Optional[str] = Field(default=None, sa_type=XMLText)

    # request_type = 'BROWSER': what to *do* in a real browser, as a list of
    # {"action": ...} steps run in order before anything is extracted --
    # goto/fill/select/click/press/wait_for/sleep/frame/eval/download. This is
    # the browser's counterpart to payload_json/payload_xml (what the request
    # *is*), which is why it sits with them and is mutually exclusive with
    # them (see ck_api_mst_behavior_browser above). Step values go through the
    # same ':KEY' and '${VAR}' substitution as a url or a payload, so a job
    # parameter can be what gets typed into a search box. See
    # app.scrapers.browser.BrowserScraper.run_behavior for the step vocabulary.
    behavior_json: Optional[list] = Field(default=None, sa_type=JSONText)

    # NULL -> response is auto-detected as XML or JSON (sniffed from
    # content-type/body, see DynamicApiScraper.parse).
    # 'zip_delimited' -> response is a binary ZIP containing one delimited
    # text file (e.g. KRX/KIS .mst master files).
    # 'delimited' -> response body itself is delimited text, no zip (a plain
    # .csv/.txt download).
    # 'binary' -> the response is a document (PDF, image, archive) that is
    # itself the result: it's staged to disk whole and only described in the
    # result table, with response_parse_json = {"group": "...", "name":
    # ":KEY.pdf"} naming where it lands. See BaseScraper._stage_binary
    # and app.services.export.stage_file/upload_file.
    # Both share response_parse_json = {"encoding": "...", "delimiter": "...",
    # "fields": ["col1", ...] (or omit + "has_header": true to read column
    # names from the file's own first line), "inner_file": "..." (zip_delimited
    # only, optional, defaults to the zip's first entry)}.
    # See DynamicApiScraper._parse_zip_delimited / _parse_delimited.
    #
    # A browser row (request_type = 'BROWSER') reads this the same way, but
    # its own set of values -- what the behavior left behind is what gets
    # read: 'dom' (default; extract from the page with the CSS spec in
    # response_parse_json), 'xhr' (keep the JSON the page itself fetched and
    # extract from that with the same selectors an HTTP row would use),
    # 'binary' (a click produced a download -- staged exactly like an HTTP
    # one). See app.scrapers.browser.
    #
    # 'session' belongs to neither engine in particular: it means "log in and
    # keep nothing but the session", and is the one response_type whose row
    # may leave output_tables_json empty. The session lands wherever that
    # transport keeps one -- the process-wide httpx cookie jar for an HTTP
    # row, a storage-state file for a browser row. A row that needs such a
    # login names it in response_parse_json: {"login": "<api_id>",
    # "logged_out": "<the tell that we were logged out>"} -- a body substring
    # for an HTTP row, a css selector for a browser row -- and the engine logs
    # in again and retries once when it sees it (see
    # app.scrapers.base.BaseScraper.collect). A login row itself may carry
    # {"expect": "<substring>"} so a rejected login fails the login job rather
    # than every job after it.
    response_type: Optional[str] = Field(default=None, max_length=20)
    response_parse_json: Optional[dict] = Field(default=None, sa_type=JSONText)

    # {source_dot_path: ENV_VAR_NAME} -- after a JSON response is parsed,
    # extract each value and persist it via app.auth_config.persist_env_var
    # (writes to both os.environ and .env on disk). For a side-effect job
    # like refreshing an OAuth token: e.g. {"access_token": "KIS_ACCESS_TOKEN"}
    # means every *later* job's "${KIS_ACCESS_TOKEN}" placeholder picks up the
    # fresh value automatically -- each systemd-timer job runs as its own
    # process, and app.auth_config reloads .env at import time, so nothing
    # else needs to be wired by hand. A job that only does this may leave
    # output_tables_json empty (see DynamicApiScraper.parse).
    persist_env_json: Optional[dict] = Field(default=None, sa_type=JSONText)

    output_tables_json: Optional[dict] = Field(default=None, sa_type=JSONText)
    # Optional: inject fields sourced from anywhere in the full response into
    # a selector's records, e.g. {"output1": {"output3.bstp_nmix_prpr": "kospi200_idx"}}
    # -- so a related value from a different top-level key lands in the same
    # saved row instead of a separate one, guaranteeing same-response/same-
    # timestamp without a join (see app.scrapers.dynamic._apply_merge_fields).
    merge_fields_json: Optional[dict] = Field(default=None, sa_type=JSONText)
    # Which of a job's params identify/key its output rows -- a property of
    # this API's own response shape (which fields the source naturally keys
    # records by), not of any particular ApiJobBuilder's parameter choices,
    # so it lives here rather than per-builder (every builder targeting the
    # same api_id needs the exact same value anyway -- confirmed empirically
    # before this field was moved here from ApiJobBuilder). See
    # app.services.job_builder's module docstring for the full format
    # (including the reserved "NOW" entry) and normalize_key_param.
    key_params_list: Optional[list] = Field(default=None, sa_type=JSONText)

    # How to keep asking for the rest of a truncated result, when one request
    # doesn't return everything. Three shapes, because APIs disagree on how to
    # say "there is more" (see app.scrapers.dynamic.fetch_all_pages):
    #
    #   {"mode": "page", "param": "PAGE", "start": 1}
    #       A page number the client increments. NULL with a ':PAGE' marker in
    #       api_url/payload means exactly this, so older rows keep working.
    #
    #   {"mode": "cursor", "param": "HHMM", "from": "output2[].stck_cntg_hour",
    #    "pick": "min", "min_records": 99}
    #       No page number -- the next request is anchored on a value read out
    #       of the answer just received (the oldest bar's timestamp, an id,
    #       ...). "min_records" is the truncation tell: a short reply means the
    #       series ran out, so stop.
    #
    #   {"mode": "token", "params": {"CTX_FK": "header:ctx_area_fk100"},
    #    "continue_when": {"source": "header:tr_cont", "in": ["F", "M"]}}
    #       An opaque continuation key handed back to be echoed, plus a flag
    #       saying whether more remains -- KIS's tr_cont/ctx_area_* pairing.
    #       Sources read "header:<name>" or a dotted body path.
    #
    # "max_pages" caps every mode as a safety net against a stop condition
    # that never fires.
    pagination_json: Optional[dict] = Field(default=None, sa_type=JSONText)

    description: Optional[str] = Field(default=None, max_length=100)
    updated_at: Optional[datetime] = Field(default=None,
        sa_column=Column(DateTime, server_default=func.now(), onupdate=func.now()))


class ApiJobBuilder(SQLModel, table=True):
    __tablename__ = "api_job_builder"

    build_id: str = Field(primary_key=True, max_length=50)
    # References ApiMst.api_id, but as a plain indexed column, not a FK (same
    # loose-coupling style as ApiRst.job_id/ApiJobLog.job_id) -- avoids the
    # constraint getting in the way during table drop/recreate migrations
    # (Oracle can't reorder columns -- see the models.py header comment).
    # Not indexed: a result table holds exactly one API's output, so api_id is
    # a single value throughout and an index on it filters nothing while
    # costing as much as a useful one -- 268MB against 5.5M rows on
    # kis_futopt_chart before it was dropped. It stays as a column for
    # traceability, not for lookup.
    api_id: str = Field(max_length=150)
    macro_params_json: dict = Field(sa_type=JSONText)
    # key_params_list moved to ApiMst -- see its field comment there.
    is_active: bool = Field(default=False, sa_type=OracleBoolean)
    save_mode: Optional[str] = Field(default="overwrite", max_length=20)
    execution_cycle: Optional[str] = Field(default="daily", max_length=20)
    description: Optional[str] = Field(default=None, max_length=100)
    updated_at: Optional[datetime] = Field(default=None,
        sa_column=Column(DateTime, server_default=func.now(), onupdate=func.now()))


class ApiJob(SQLModel, table=True):
    __tablename__ = "api_job"

    job_id: str = Field(primary_key=True, max_length=150)
    # References ApiJobBuilder.build_id / ApiMst.api_id, but as plain indexed
    # columns, not FKs (same loose-coupling style as ApiRst.job_id/
    # ApiJobLog.job_id) -- avoids the constraint getting in the way during
    # table drop/recreate migrations (Oracle can't reorder columns -- see
    # the models.py header comment).
    build_id: Optional[str] = Field(default=None, index=True, max_length=50)
    # Not indexed: a result table holds exactly one API's output, so api_id is
    # a single value throughout and an index on it filters nothing while
    # costing as much as a useful one -- 268MB against 5.5M rows on
    # kis_futopt_chart before it was dropped. It stays as a column for
    # traceability, not for lookup.
    api_id: str = Field(max_length=150)
    params_json: dict = Field(sa_type=JSONText)
    is_active: bool = Field(default=True, sa_type=OracleBoolean)
    save_mode: Optional[str] = Field(default="overwrite", max_length=20)
    execution_cycle: Optional[str] = Field(default="daily", max_length=20)
    description: Optional[str] = Field(default=None, max_length=100)
    updated_at: Optional[datetime] = Field(default=None,
        sa_column=Column(DateTime, server_default=func.now(), onupdate=func.now()))


class ApiJobLog(SQLModel, table=True):
    """Append-only execution history for an ApiJob (one row per run attempt).

    job_id is a plain indexed column, not a FK (same loose-coupling style as
    ApiRst.job_id) -- keeps log inserts/deletes independent of api_job."""

    __tablename__ = "api_job_log"

    id: Optional[int] = Field(default=None, sa_column=Column(Integer, Identity(start=1), primary_key=True))
    job_id: str = Field(index=True, max_length=150)
    status: str = Field(max_length=20)
    error_message: Optional[str] = Field(default=None, max_length=4000)
    executed_at: Optional[datetime] = Field(default=None, sa_column=Column(DateTime))


class ApiRst(ResultTable, table=True):
    """Generalized Output Table for API Data"""
    __tablename__ = "api_rst"

    id: Optional[int] = Field(default=None, sa_column=Column(Integer, Identity(start=1), primary_key=True))
    # Indexed here, unlike the typed result tables: this one is shared by
    # every API whose output has no table of its own, so api_id genuinely
    # selects a subset (6 values across ~114k rows) instead of matching
    # everything.
    api_id: str = Field(index=True, max_length=150)
    job_id: str = Field(index=True, max_length=150)

    # The subset of the generating ApiJob's params_json named in that job's
    # ApiMst.key_params_list (see app.services.execution.run_job).
    # Generic table shared across many APIs with different key param names,
    # so this stays a flexible JSON column rather than fixed columns (contrast
    # with a dedicated table like KisFutoptChart, which gets real columns).
    key_params_json: Optional[dict] = Field(default=None, sa_type=JSONText)

    result_json: dict = Field(default_factory=dict, sa_type=JSONText)

    updated_at: Optional[datetime] = Field(default=None,
                sa_column=Column(DateTime, server_default=func.now(), onupdate=func.now()))
