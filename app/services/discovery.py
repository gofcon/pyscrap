"""Backfills contracts for periods that predate the instrument master files.

KIS publishes only *currently listed* instruments in its master file, so a
contract that has already expired cannot be looked up -- its short_code has to
be reconstructed. That code is not opaque: it is

    [side][product][mat_scd][strike]

    side      'A' futures, 'B'/'C' call/put, or '2'/'3' for maturities up to
              2025-12 (KIS renumbered the whole scheme at the 2026-01 expiry,
              switching both this prefix and the mat_scd format at once).
    product   '01' K2I, '05' MKI, '09' WKI, 'AF' WKM.
    mat_scd   the maturity's counter code, read from MetaMaturity -- never
              derived, since the counter skips I/O/U, changed format in 2026,
              and occasionally repeats a code when two maturities settle on
              the same day.
    strike    FLOOR(strike) as 3 digits. Exact below 1000; at and above it KIS
              switches to a listing-order code that cannot be computed, which
              is why this only covers periods where the index was under 1000.

Everything except the strike is known, so discovery means walking the strike
ladder (a fixed 2.5 grid) outward from the money and asking the daily-chart
endpoint which ones answer: a contract that existed returns bars, one that
never listed returns none. The walk stops after enough consecutive misses to
tolerate a ladder that thins out at the edges.

Going forward this is unnecessary -- the daily master files cover new
contracts -- so rows created here are tagged (see ORIGIN) to keep the
reconstructed set distinguishable from the authoritative one.
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any, Iterable

from loguru import logger
from sqlalchemy import text
from sqlmodel import Session

from app.db.models import KisFutoptDaily, MstFuopt
from app.scrapers.dynamic import _pace_host, get_http_client

API_ID = "KIS_FUTOPT_DAILY"

# Marks a row as reconstructed rather than read from a master file. Kept short
# enough to fit MstFuopt.description and distinct from the snapshot loader's
# own marker, so the two sets stay separable after the fact.
ORIGIN = "reconstructed by probe (no master file)"

PRODUCT_CODE = {"K2I": "01", "MKI": "05", "WKI": "09", "WKM": "AF"}

# First maturity on the post-renumbering scheme (2026-01 expiry). At and after
# this date a call is 'B' and a put 'C'; before it, '2' and '3'. Verified
# against the endpoint across 2019-2026: the old prefixes stop answering
# exactly here, in step with the mat_scd format change.
PREFIX_SWITCH = date(2026, 1, 8)

STRIKE_STEP = 2.5
STRIKE_FLOOR = 100.0        # below this no index option has ever listed
STRIKE_CEIL = 1000.0        # at/above this the strike code stops being computable
MISS_LIMIT = 5              # consecutive empty replies that end a walk


def _side_prefix(is_call: bool, mat_date: date) -> str:
    if mat_date >= PREFIX_SWITCH:
        return "B" if is_call else "C"
    return "2" if is_call else "3"


def build_short_code(prod_type: str, mat_scd: str, mat_date: date,
                     strike: float | None, is_call: bool = True) -> str:
    """Assemble the KIS short_code for one contract. ``strike=None`` builds the
    futures code, which has no strike segment (6 chars instead of 9)."""
    # Futures carry no side: 'A' regardless of the call/put argument, which is
    # meaningless for them. Checking the strike rather than a separate flag
    # keeps the two facts that distinguish a future -- no side, no strike --
    # from being able to disagree.
    side = "A" if strike is None else _side_prefix(is_call, mat_date)
    head = f"{side}{PRODUCT_CODE[prod_type]}{mat_scd}"
    return head if strike is None else f"{head}{int(strike):03d}"


def _fetch_bars(short_code: str, start: date, end: date) -> list[dict[str, Any]]:
    """Daily bars for one contract, or [] if it never listed. The endpoint
    answers for delisted codes -- that is what makes discovery possible -- and
    reports a non-existent one as an empty output2 rather than an error, so an
    empty result is the 'no such contract' signal."""
    api_url = (
        "${KIS_PROD}/uapi/domestic-futureoption/v1/quotations/inquire-daily-fuopchartprice"
        f"?fid_cond_mrkt_div_code=O&fid_input_iscd={short_code}"
        f"&fid_input_date_1={start:%Y%m%d}&fid_input_date_2={end:%Y%m%d}&fid_period_div_code=D"
    )
    from app.auth_config import resolve_env_placeholders

    url = resolve_env_placeholders(api_url)
    headers = resolve_env_placeholders(
        '{"content-type": "application/json", "authorization": "Bearer ${KIS_ACCESS_TOKEN}",'
        ' "appkey": "${KIS_APP_KEY}", "appsecret": "${KIS_APP_SECRET}"}'
    )
    import json as _json

    hdr = _json.loads(headers)
    hdr["tr_id"] = "FHKIF03020100"
    _pace_host(url)
    payload = get_http_client().get(url, headers=hdr).json()
    return [b for b in (payload.get("output2") or []) if b.get("stck_bsop_date")]


def _existing_codes(session: Session, job_id: str) -> set[str]:
    """short_codes already collected for this maturity. Discovery is resumable
    at contract granularity: a re-run skips these instead of re-asking, which
    matters because a full backfill is tens of thousands of paced requests."""
    rows = session.exec(
        text("SELECT DISTINCT short_code FROM kis_futopt_daily WHERE job_id = :j"),
        params={"j": job_id},
    ).all()
    return {r[0] for r in rows}


def _walk_strikes(atm: float) -> Iterable[float]:
    """Strike ladder outward from the money: at-the-money, then alternating up
    and down. Interleaved rather than one side at a time so a maturity that is
    cut short still ends up with the most useful contracts."""
    yield atm
    offset = STRIKE_STEP
    while True:
        up, down = atm + offset, atm - offset
        stop = True
        if up < STRIKE_CEIL:
            yield up
            stop = False
        if down >= STRIKE_FLOOR:
            yield down
            stop = False
        if stop:
            return
        offset += STRIKE_STEP


def discover_maturity(session: Session, prod_type: str, mat_code: str, mat_scd: str,
                      mat_date: date, front_date: date | None, atm: float) -> dict[str, int]:
    """Probe one maturity's strike ladder, saving every contract that answers.

    Returns {'probed': n, 'found': n, 'bars': n, 'skipped': n}."""
    job_id = f"BACKFILL_{prod_type}_{mat_code}"
    start = front_date or (mat_date - timedelta(days=60))
    done = _existing_codes(session, job_id)
    stats = {"probed": 0, "found": 0, "bars": 0, "skipped": 0}

    for is_call in (True, False):
        misses = 0
        for strike in _walk_strikes(atm):
            if misses >= MISS_LIMIT:
                break
            code = build_short_code(prod_type, mat_scd, mat_date, strike, is_call)
            if code in done:
                stats["skipped"] += 1
                misses = 0
                continue
            stats["probed"] += 1
            bars = _fetch_bars(code, start, mat_date)
            if not bars:
                misses += 1
                continue
            misses = 0
            stats["found"] += 1
            stats["bars"] += len(bars)
            session.add(MstFuopt(
                short_code=code, prod_nm=f"{prod_type} {mat_code} {strike}",
                prod_type=prod_type, call_put_cd="CALL" if is_call else "PUT",
                ul_code="2001", ul_nm="KOSPI200", cont_mult=250000.0,
                mat_code=mat_code, mat_date=mat_date, front_date=front_date,
                strike_prc=strike, description=ORIGIN, updated_at=None,
            ))
            for bar in bars:
                session.add(KisFutoptDaily(
                    api_id=API_ID, job_id=job_id, short_code=code,
                    stck_bsop_date=bar["stck_bsop_date"],
                    futs_prpr=_num(bar.get("futs_prpr")), futs_oprc=_num(bar.get("futs_oprc")),
                    futs_hgpr=_num(bar.get("futs_hgpr")), futs_lwpr=_num(bar.get("futs_lwpr")),
                    acml_vol=_int(bar.get("acml_vol")), acml_tr_pbmn=_int(bar.get("acml_tr_pbmn")),
                    mod_yn=bar.get("mod_yn"),
                ))
            session.commit()
    return stats


def _num(v: Any) -> float | None:
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _int(v: Any) -> int | None:
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return None


def discover_period(session: Session, period: str) -> dict[str, dict[str, int]]:
    """Backfill every maturity settling in ``period`` ("2019" or "2019-10").

    Scoped by period because a full backfill is hours of paced requests --
    small, restartable chunks keep a failure from costing the whole run, and
    the natural chunk is the maturity, since a strike ladder only makes sense
    relative to where the index sat when that contract settled."""
    like = f"{period}%" if len(period) == 4 else f"{period[:4]}-{period[5:7]}%"
    rows = session.exec(text("""
        SELECT m.prod_type, m.mat_code, m.mat_scd, m.mat_date, m.prev_mat_date,
               (SELECT h.close_price FROM stock_index_his h
                 WHERE h.mv_id = 'KI2'
                   AND h.trade_date = (SELECT MAX(trade_date) FROM stock_index_his
                                        WHERE mv_id = 'KI2' AND trade_date < m.mat_date)) ref
          FROM meta_maturity m
         WHERE m.mat_scd IS NOT NULL AND m.mat_date IS NOT NULL
           AND TO_CHAR(m.mat_date, 'YYYY-MM') LIKE :p
         ORDER BY m.mat_date, m.prod_type"""), params={"p": like}).all()

    results: dict[str, dict[str, int]] = {}
    for prod_type, mat_code, mat_scd, mat_date, front_date, ref in rows:
        key = f"{prod_type} {mat_code}"
        if ref is None:
            logger.warning("{}: no index close before {}, skipped", key, mat_date)
            continue
        atm = round(float(ref) / STRIKE_STEP) * STRIKE_STEP
        stats = discover_maturity(session, prod_type, mat_code, mat_scd,
                                  mat_date.date() if hasattr(mat_date, "date") else mat_date,
                                  front_date.date() if hasattr(front_date, "date") else front_date,
                                  atm)
        results[key] = stats
        logger.info("{}: atm={} probed={} found={} bars={} skipped={}",
                    key, atm, stats["probed"], stats["found"], stats["bars"], stats["skipped"])
    return results
