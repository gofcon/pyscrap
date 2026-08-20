from collections.abc import Generator
from typing import Any

from sqlalchemy.engine import URL
from sqlmodel import Session, create_engine

from app.db_config import settings

_connect_args: dict[str, Any] = {"dsn": settings.oracle_dsn}
if settings.oracle_wallet_dir:
    # mTLS wallet connection (see Settings.oracle_wallet_dir's comment) --
    # config_dir lets python-oracledb resolve settings.oracle_dsn as a TNS
    # alias from the wallet's tnsnames.ora instead of a raw connect
    # descriptor; wallet_location points at the same directory for the
    # client certificate. cwallet.sso is an auto-login wallet, so no
    # wallet_password is needed here.
    _connect_args["config_dir"] = settings.oracle_wallet_dir
    _connect_args["wallet_location"] = settings.oracle_wallet_dir
    # Required in practice, even though python-oracledb's own signature
    # allows None -- see Settings.oracle_wallet_password's comment for what
    # omitting it actually does (hangs, doesn't error).
    _connect_args["wallet_password"] = settings.oracle_wallet_password

_url = URL.create(
    "oracle+oracledb",
    username=settings.oracle_user,
    password=settings.oracle_password,
)
engine = create_engine(_url, connect_args=_connect_args, echo=False)


def get_session() -> Generator[Session, None, None]:
    with Session(engine) as session:
        yield session
