"""CLI shim forwarding ``python3 -m app.cli`` to ``app.v3.cli``."""

from __future__ import annotations

import sys

from app.v3.cli import main

if __name__ == "__main__":
    sys.exit(main())
