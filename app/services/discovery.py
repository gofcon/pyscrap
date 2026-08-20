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

# How far in-the-money to keep. Deep ITM options barely trade and their value
# is nearly all intrinsic, so the ladder is only carried a few steps past the
# money; the out-of-the-money side is mapped in full.
ITM_STEPS = 5

# Offsets (in index points) tried outward from the money before bisecting, to
# bracket the far OTM edge. Doubling rather than stepping because the ladder
# runs hundreds of points wide and its width scales with the index level.
BRACKET_OFFSETS = (25.0, 50.0, 100.0, 200.0, 400.0)

# Steps probed past a converged edge before accepting it. A contract that
# listed but never traded answers empty, indistinguishable from one that never
# listed, so a lone silent strike would otherwise cut the ladder short --
# observed on 5 of 325 strikes in a sample. Re-probing past the edge turns
# that into a gap to step over rather than a wall.
EDGE_CONFIRM_STEPS = 3


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



def _snap(strike: float) -> float:
    """Round onto the 2.5 ladder every index option strike sits on."""
    return round(strike / STRIKE_STEP) * STRIKE_STEP


def _listed(prod_type: str, mat_scd: str, mat_date: date, front_date: date | None,
            strike: float, is_call: bool) -> bool:
    if not (STRIKE_FLOOR <= strike < STRIKE_CEIL):
        return False
    code = build_short_code(prod_type, mat_scd, mat_date, strike, is_call)
    start = front_date or (mat_date - timedelta(days=60))
    return bool(_fetch_bars(code, start, mat_date))


def find_otm_edge(prod_type: str, mat_scd: str, mat_date: date, front_date: date | None,
                  atm: float, is_call: bool) -> tuple[float, int]:
    """Outermost listed strike on the out-of-the-money side, and how many
    probes it took.

    Bracket by doubling offsets until one comes back empty, bisect the bracket
    down to a single ladder step, then step past the result a few times in
    case the edge was really a silent strike. Bisection rather than a walk
    because the ladder is uniform: once the edge is known every strike inside
    it follows arithmetically, so only the boundary has to be asked about.

    Direction follows moneyness -- OTM is above the money for a call, below it
    for a put."""
    sign = 1.0 if is_call else -1.0
    probe = lambda k: _listed(prod_type, mat_scd, mat_date, front_date, k, is_call)
    calls = 0

    lo = atm                      # known listed (or the money itself)
    hi = None                     # first offset that came back empty
    for offset in BRACKET_OFFSETS:
        candidate = _snap(atm + sign * offset)
        calls += 1
        if probe(candidate):
            lo = candidate
        else:
            hi = candidate
            break
    if hi is None:
        hi = _snap(atm + sign * (BRACKET_OFFSETS[-1] + STRIKE_STEP))

    while abs(hi - lo) > STRIKE_STEP:
        mid = _snap((lo + hi) / 2)
        if mid in (lo, hi):
            break
        calls += 1
        if probe(mid):
            lo = mid
        else:
            hi = mid

    edge = lo
    for step in range(1, EDGE_CONFIRM_STEPS + 1):
        candidate = _snap(edge + sign * STRIKE_STEP * step)
        calls += 1
        if probe(candidate):
            edge = candidate
            # A silent strike hid a live one past it: re-open the search from here.
            for extra in range(1, EDGE_CONFIRM_STEPS + 1):
                nxt = _snap(edge + sign * STRIKE_STEP * extra)
                calls += 1
                if probe(nxt):
                    edge = nxt
    return edge, calls


def _strike_range(lo: float, hi: float) -> list[float]:
    out, k = [], lo
    while k <= hi + 1e-9:
        out.append(round(k, 1))
        k += STRIKE_STEP
    return out


def discover_maturity(session: Session, prod_type: str, mat_code: str, mat_scd: str,
                      mat_date: date, front_date: date | None, atm: float,
                      mirror_to: Iterable[str] = ()) -> dict[str, int]:
    """Map one maturity's strike ladder and record the contracts on it.

    Only the two OTM edges are asked about; everything between them is derived,
    since the ladder is a uniform 2.5 grid. That also recovers contracts a
    per-strike probe would miss -- one that listed but never traded answers
    empty and would look non-existent.

    ``mirror_to`` copies the resulting strikes to other products that share
    this expiry calendar and ladder (mini against the monthly), sparing a
    second identical search."""
    call_edge, c1 = find_otm_edge(prod_type, mat_scd, mat_date, front_date, atm, True)
    put_edge, c2 = find_otm_edge(prod_type, mat_scd, mat_date, front_date, atm, False)

    itm_span = STRIKE_STEP * ITM_STEPS
    calls = _strike_range(max(put_edge, _snap(atm - itm_span)), call_edge)
    puts = _strike_range(put_edge, min(call_edge, _snap(atm + itm_span)))

    # Covers the mirror targets too, not just prod_type: their rows are written
    # here as well, so leaving them out of the seen-set lets a re-run collide on
    # the primary key instead of skipping what it already produced.
    targets = (prod_type, *mirror_to)
    placeholders = ", ".join(f":p{i}" for i in range(len(targets)))
    params: dict[str, Any] = {f"p{i}": t for i, t in enumerate(targets)}
    params["m"] = mat_code
    existing = {r[0] for r in session.exec(
        text(f"SELECT short_code FROM mst_fuopt WHERE prod_type IN ({placeholders}) AND mat_code = :m"),
        params=params).all()}

    added = 0
    for target in targets:
        for is_call, strikes in ((True, calls), (False, puts)):
            for strike in strikes:
                code = build_short_code(target, mat_scd, mat_date, strike, is_call)
                if code in existing:
                    continue
                existing.add(code)
                session.add(MstFuopt(
                    short_code=code, prod_nm=f"{target} {mat_code} {strike}",
                    prod_type=target, call_put_cd="CALL" if is_call else "PUT",
                    ul_code="2001", ul_nm="KOSPI200", cont_mult=250000.0,
                    mat_code=mat_code, mat_date=mat_date, front_date=front_date,
                    strike_prc=strike, description=ORIGIN, updated_at=None,
                ))
                added += 1
    session.commit()
    return {"probed": c1 + c2, "call_edge": call_edge, "put_edge": put_edge,
            "contracts": added, "calls": len(calls), "puts": len(puts)}


MIRROR = {"K2I": ("MKI",)}   # mini shares the monthly calendar and the same ladder


def discover_period(session: Session, period: str) -> dict[str, dict[str, Any]]:
    """Backfill every maturity settling in ``period`` ("2019" or "2019-10").

    Scoped by period because a strike ladder is only meaningful relative to
    where the index sat when that contract settled, and because small
    restartable chunks keep one failure from costing a whole run.

    MKI is excluded from the scan and filled by mirroring K2I -- the two share
    an expiry calendar and a ladder, so searching both would double the cost
    for an identical answer."""
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
           AND m.prod_type <> 'MKI'
         ORDER BY m.mat_date, m.prod_type"""), params={"p": like}).all()

    results: dict[str, dict[str, Any]] = {}
    for prod_type, mat_code, mat_scd, mat_date, front_date, ref in rows:
        key = f"{prod_type} {mat_code}"
        if ref is None:
            logger.warning("{}: no index close before {}, skipped", key, mat_date)
            continue
        stats = discover_maturity(
            session, prod_type, mat_code, mat_scd,
            mat_date.date() if hasattr(mat_date, "date") else mat_date,
            front_date.date() if hasattr(front_date, "date") else front_date,
            _snap(float(ref)), mirror_to=MIRROR.get(prod_type, ()))
        results[key] = stats
        logger.info("{}: probed={} edges {}..{} contracts={}",
                    key, stats["probed"], stats["put_edge"], stats["call_edge"], stats["contracts"])
    return results
