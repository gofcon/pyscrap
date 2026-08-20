"""Quick standalone connection test against the Oracle DB configured in .env."""

import oracledb

from app.db_config import settings


def main() -> None:
    print(f"Connecting as {settings.oracle_user} via thin mode...")
    with oracledb.connect(
        user=settings.oracle_user,
        password=settings.oracle_password,
        dsn=settings.oracle_dsn,
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT banner FROM v$version WHERE rownum = 1")
            print("Connected. DB version:", cursor.fetchone()[0])


if __name__ == "__main__":
    main()
