#!/bin/zsh
cd -- "$(dirname -- "$0")" || exit 1
.venv/bin/python scripts/run_dashboard.py
