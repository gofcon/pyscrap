"""Ad-hoc single run entrypoint, e.g. for manual testing or a scheduled task."""

from app.cli import app

if __name__ == "__main__":
    app()
