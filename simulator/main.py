"""Back-compat entrypoint: ``uv run python main.py`` → package CLI."""

from __future__ import annotations

import sys

from simulator.cli import main

if __name__ == "__main__":
    sys.exit(main())
