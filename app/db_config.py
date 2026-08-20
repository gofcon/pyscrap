from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # extra="ignore": .env is shared with per-site scraper secrets (e.g.
    # KRX_AUTH_KEY, see app.auth_config) that aren't declared as Settings
    # fields -- those are resolved from os.environ directly instead, so
    # Settings must tolerate seeing them in the file without erroring.
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

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
