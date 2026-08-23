"""Compare scripts/sql against what the database actually compiled.

The procedures in scripts/sql are not loaded by this program -- Python calls
the compiled objects by name (see app.cli._call_procedure), and the files are
the reviewed record of what those objects should be. That record is worth
keeping for what the database cannot hold: why a change was made, a diff to
review it by, and a way to build the objects somewhere that has none. All
three assume the file still matches the database, and nothing enforces that,
so the assumption needs checking rather than trusting.

It has already failed once: a target added straight into sp_run_export was
absent from the file, so redeploying the file would have silently dropped it,
and the type error in that target reached the batch without ever being read.

The database stays the source of truth -- editing a procedure there is the
point of splitting the work -- and this reports what the file is missing, with
pull() to bring it back so the change can be committed with its reason.
"""
import re
from dataclasses import dataclass
from pathlib import Path

from loguru import logger
from sqlalchemy import text
from sqlmodel import Session

from app.db.engine import engine

SQL_DIR = Path("scripts/sql")

# Objects whose source the database keeps verbatim in user_source, which is
# what makes an exact comparison possible. Views and materialized views are
# deliberately absent: the database keeps only the query, reformatted and
# without the DDL around it, so comparing them would report a difference on
# every run and teach everyone to ignore this command.
_CREATE_RE = re.compile(
    r"CREATE\s+(?:OR\s+REPLACE\s+)?"
    r"(PROCEDURE|FUNCTION|PACKAGE(?:\s+BODY)?|TRIGGER|TYPE(?:\s+BODY)?)\s+"
    r"([A-Za-z0-9_$#]+)",
    re.IGNORECASE)


@dataclass
class Comparison:
    name: str                 # object name, or the file name when there is none
    status: str               # match | differ | missing | orphan | skipped
    detail: str = ""
    path: Path | None = None


def _normalize(source: str) -> str:
    """Line endings and trailing spaces removed -- differences no reviewer
    would call a difference."""
    return "\n".join(line.rstrip() for line in source.replace("\r\n", "\n").split("\n")).strip()


def _from_name(source: str, name: str) -> str:
    """The source from the object's name onward.

    The header is where the two sides legitimately disagree: the database
    stores what follows CREATE OR REPLACE, blanking words like EDITIONABLE
    rather than removing them, so its first line carries spacing the file
    never had. Everything from the name on is kept exactly as written."""
    position = source.upper().find(name.upper())
    return source[position:] if position >= 0 else source


def _db_source(session: Session, name: str) -> str | None:
    rows = session.exec(text(
        "SELECT text FROM user_source WHERE name = :n ORDER BY line"
    ).bindparams(n=name.upper())).all()
    return "".join(t for t, in rows) if rows else None


def compare() -> list[Comparison]:
    """One result per file in scripts/sql, plus one per compiled object that
    no file covers -- drift has two directions and only reporting the first
    would leave an object with no record at all looking clean."""
    results: list[Comparison] = []
    covered: set[str] = set()

    with Session(engine) as session:
        for path in sorted(SQL_DIR.glob("*.sql")):
            source = path.read_text(encoding="utf-8")
            match = _CREATE_RE.search(source)
            if not match:
                results.append(Comparison(path.name, "skipped",
                                          "not a CREATE for an object the database stores verbatim",
                                          path))
                continue

            name = match.group(2)
            covered.add(name.upper())
            db_source = _db_source(session, name)
            if db_source is None:
                results.append(Comparison(name, "missing", "not compiled in this database", path))
                continue

            on_file = _normalize(_from_name(source[match.start(2):], name))
            in_db = _normalize(_from_name(db_source, name))
            if on_file == in_db:
                results.append(Comparison(name, "match", path=path))
            else:
                results.append(Comparison(
                    name, "differ",
                    f"database {len(in_db.splitlines())} lines, file {len(on_file.splitlines())}",
                    path))

        for name, kind in session.exec(text(
            "SELECT object_name, object_type FROM user_objects "
            " WHERE object_type IN ('PROCEDURE','FUNCTION','PACKAGE','PACKAGE BODY','TRIGGER') "
            " ORDER BY object_name")).all():
            if name.upper() not in covered:
                results.append(Comparison(name, "orphan", f"{kind.lower()} with no file"))

    return results


def pull(name: str) -> Path:
    """Write the database's version of one object over its file, so a change
    made in the database can be committed with the reason for it."""
    path = SQL_DIR / f"{name.lower()}.sql"
    with Session(engine) as session:
        source = _db_source(session, name)
    if source is None:
        raise LookupError(f"{name} is not compiled in this database")

    path.write_text("CREATE OR REPLACE " + _normalize(source) + "\n", encoding="utf-8")
    logger.info("pulled {} into {}", name, path)
    return path
