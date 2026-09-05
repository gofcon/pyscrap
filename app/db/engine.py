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
# pool_pre_ping: 풀에서 꺼낸 커넥션을 쓰기 전에 한 번 찔러 보고, 죽어 있으면
# 조용히 새로 맺는다. 없으면 오래 놀린 커넥션이 그대로 반환되어 첫 질의가
# DPY-4011("the database or network closed the connection")로 죽는다 --
# Autonomous DB 든 그 앞의 네트워크든 유휴 커넥션을 끊고, 풀은 그 사실을 모른다.
#
# 짧게 살다 죽는 배치에서는 잘 안 드러나지만, 오래 도는 프로세스에서는 확실히
# 문제가 된다: KRX 과거 이행 스크립트가 배치 시간대를 피해 2시간 반 자고 일어난
# 직후 첫 질의에서 그렇게 죽었다(2026-09-02, 한 건도 못 받고).
#
# 값은 커넥션을 꺼낼 때마다 왕복 한 번이다. 이 앱의 질의는 그보다 훨씬 무겁고,
# 대안(pool_recycle 로 시간을 찍는 것)은 상대의 타임아웃을 추측해야 한다.
engine = create_engine(_url, connect_args=_connect_args, echo=False, pool_pre_ping=True)


def get_session() -> Generator[Session, None, None]:
    with Session(engine) as session:
        yield session
