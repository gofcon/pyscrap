from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # extra="ignore": .env is shared with per-site scraper secrets (e.g.
    # KRX_AUTH_KEY, see app.auth_config) that aren't declared as Settings
    # fields -- those are resolved from os.environ directly instead, so
    # Settings must tolerate seeing them in the file without erroring.
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # 엔진이 쓰는 계정 -- v2 에서는 SCRAP 쪽이다. 접미사 없는 이름이 그대로
    # 남은 것이 의도다: v2 는 **별도 인스턴스**이고 계정 이름과 자격은 전과
    # 같아서, 가리키는 곳은 oracle_dsn 이 정한다. 그래서 두 층 전환에 이
    # 파일도 app.db.engine 도 고칠 것이 없었다.
    #
    # .env 에는 ORACLE_USER_REF / ORACLE_PASSWORD_REF 도 있는데 여기 필드로
    # 두지 않는다. 엔진이 REF 로 접속할 일이 없기 때문이다 -- 사전 데이터는
    # synonym 으로 읽고(06_grants_synonyms.sql), 동기화 프로시저도 SCRAP 에서
    # synonym 으로 불린다(실측: 10개 전부 호출된다). REF 자격이 필요한 것은
    # 그 계정 소유 객체를 만들거나 MV 를 갱신하는 관리 작업뿐이고, 그것은
    # 앱 경로가 아니다. 아무도 읽지 않는 필드를 선언하면 "엔진이 REF 로도
    # 붙는다" 는 오해만 남는다.
    oracle_user: str
    oracle_password: str
    # Easy Connect string, a full (description=...) connect descriptor, or --
    # when oracle_wallet_dir is set -- a TNS alias from that wallet's
    # tnsnames.ora (e.g. "..._medium"). See app.db.engine.
    oracle_dsn: str
    # Path to an unzipped Oracle ADB wallet directory (cwallet.sso,
    # tnsnames.ora, sqlnet.ora, ...) for mTLS auth -- independent of the
    # client's source IP, unlike the plain-TLS + ACL connection this
    # replaces (see app.db.engine). None keeps the old ACL/plain-TLS
    # behavior. Never commit an actual wallet directory to git (see
    # .gitignore) -- it's bundled into the deploy artifact instead (see
    # .fastapicloudignore), the same way a TLS cert normally ships with a
    # server rather than being treated as a per-request secret.
    oracle_wallet_dir: str | None = None
    # The password set when the wallet was downloaded from OCI. Required for
    # python-oracledb's thin mode, which reads ewallet.pem (not cwallet.sso/
    # ewallet.p12, those are thick-mode-only formats) -- verified live: with
    # no wallet_password, oracledb.connect() hangs indefinitely instead of
    # erroring (looks like a blocking prompt-for-input path with no
    # interactive stdin available), while *any* string (even a wrong one)
    # avoids the hang and gets a normal fast connect attempt/error instead.
    oracle_wallet_password: str | None = None
    log_level: str = "INFO"


settings = Settings()
