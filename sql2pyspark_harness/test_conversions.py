"""pytest entry point: one test per query in the suite.

    pytest -q                          # all queries
    pytest -q -k "Q08 or Q12"          # a subset
    SQL2PYSPARK_CONVERSIONS=out/ pytest -q   # point at your converter's output

Queries without a conversion file are reported as SKIPPED.
"""
import os
from pathlib import Path

import pytest

from compare_harness import (
    MISSING, PASS, Options, compare_one, load_conversion, load_suite, make_spark, run_setup,
)

HERE = Path(__file__).parent
SQL_FILE = Path(os.environ.get("SQL2PYSPARK_SQL", HERE / "sql2pyspark_test_suite.sql"))
CONV_DIR = Path(os.environ.get("SQL2PYSPARK_CONVERSIONS", HERE / "conversions"))

STATEMENTS = load_suite(SQL_FILE)
QUERIES = [s for s in STATEMENTS if not s.is_setup]
OPTIONS = Options(
    float_places=int(os.environ.get("SQL2PYSPARK_FLOAT_PLACES", 6)),
    sort_arrays=os.environ.get("SQL2PYSPARK_SORT_ARRAYS") == "1",
    ordered=os.environ.get("SQL2PYSPARK_ORDERED") == "1",
)


@pytest.fixture(scope="session")
def spark():
    session = make_spark()
    run_setup(session, STATEMENTS)
    yield session
    session.stop()


@pytest.mark.parametrize("stmt", QUERIES, ids=[q.id for q in QUERIES])
def test_conversion_matches_sql(spark, stmt):
    result = compare_one(spark, stmt, load_conversion(CONV_DIR, stmt.id), OPTIONS)
    if result.status == MISSING:
        pytest.skip(f"no conversion for {stmt.id}")
    detail = "\n".join(
        [result.message]
        + [f"- only in SQL:     {r}" for r in result.only_in_sql]
        + [f"+ only in PySpark: {r}" for r in result.only_in_pyspark]
    )
    assert result.status == PASS, f"{result.status}\n{detail}"
