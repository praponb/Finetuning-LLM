"""Point the classic generators at the synthetic catalog.

generate_dataset.py, advanced.py and ../targeted_v11/generate.py read their tables from `schemas` when they are
imported (`TABLES = train_tables()`, `TABLE_BY_NAME`, `extra_train_tables()`), and verify_pairs.Checker builds its
DataFrames from `schemas.TABLES + schemas.EXTRA_TABLES`. install() replaces those with synthetic tables, so it must run
BEFORE any of those modules is imported (it refuses otherwise).
"""
from __future__ import annotations

import sys

GENERATORS = ("generate_dataset", "advanced", "gen_v11", "generate")


def install(tables, extra=()) -> None:
    loaded = [m for m in GENERATORS if m in sys.modules]
    if loaded:
        raise RuntimeError(f"install() must run before importing {loaded}")
    import schemas
    schemas.TABLES = tuple(tables)
    schemas.EXTRA_TABLES = tuple(extra)
    schemas.TABLE_BY_NAME.clear()
    schemas.TABLE_BY_NAME.update({t.name: t for t in schemas.TABLES + schemas.EXTRA_TABLES})


def install_all() -> None:
    """every synthetic table (training + held-out domains; nested tables as EXTRA_TABLES), for the verifier"""
    import synth_catalog as sc
    install([t for v in sc.CATALOG.values() for t in v["tables"]], [v["nested"] for v in sc.CATALOG.values()])


def init_verify_worker() -> None:
    """Pool initializer for verify.py: spawned workers start without the patch, so apply it before the Checker"""
    install_all()
    import verify_pairs
    verify_pairs._init_worker()
