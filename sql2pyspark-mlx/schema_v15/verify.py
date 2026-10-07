"""Execution check for the v15 pairs: ../verify_pairs.py unchanged, with the synthetic catalog in place of the
classic tables (in the main process and in every spawned worker).

  python verify.py --src raw_v15 --workers 5   -> raw_v15/verified.jsonl, verify_report.json, verify_failures.jsonl
"""
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path[:0] = [str(HERE), str(HERE.parent)]

import patch  # noqa: E402

if __name__ == "__main__":
    patch.install_all()
    import verify_pairs
    verify_pairs._init_worker = patch.init_verify_worker
    verify_pairs.main()
