# sql2pyspark comparison harness

Checks a Spark SQL → PySpark converter by running every query in the test suite
both ways and confirming that the two results are identical.

```
sql2pyspark_harness/
├── compare_harness.py           # the harness (CLI + library)
├── test_conversions.py          # pytest wrapper, one test per query
├── sql2pyspark_test_suite.sql   # the SQL suite, one "-- @id: <ID>" marker per statement
└── conversions/                 # one <ID>.py per query, each with build(spark) -> DataFrame
    ├── Q01.py  Q03.py  Q04a.py  Q05a.py  Q06b.py  Q07.py  Q08.py  Q10a.py   (hand-written references)
```

Requires PySpark 3.4+ (4.0+ for the `groupingSets` reference in Q04a) and Java 17+.

## Run it

```bash
pip install pyspark pytest

# CLI
python compare_harness.py --sql sql2pyspark_test_suite.sql --conversions conversions
python compare_harness.py ... --only Q01,Q08 --show-diff 10
python compare_harness.py ... --report results.json --junit junit.xml   # for CI

# pytest
pytest -q
SQL2PYSPARK_CONVERSIONS=path/to/converter_output pytest -q
```

## Plugging in your converter

For each query ID, have the converter write `<ID>.py` into a folder:

```python
from pyspark.sql import SparkSession, DataFrame, functions as F

def build(spark: SparkSession) -> DataFrame:
    orders = spark.table("orders")      # the setup views already exist
    ...
    return result_df
```

Point `--conversions` (or `SQL2PYSPARK_CONVERSIONS`) at that folder. Queries with no
file are reported as `MISSING` (shown as skipped in pytest) until the converter covers them.

## What gets compared

| Check  | Rule | Relax with |
|--------|------|------------|
| Schema | same column names in order, same Spark data types | `--ignore-names`, `--loose-types` |
| Data   | same multiset of rows (duplicates count). Doubles rounded to 6 places, maps compared with keys sorted, nested structs and arrays compared recursively | `--float-places N`, `--sort-arrays` |
| Order  | off by default, because `ORDER BY` with ties is not deterministic | turn on with `--ordered` |

Every value of every row is computed on both sides. A plain `count()` would let Spark
skip unused columns and hide runtime errors (ANSI overflow, out-of-range array index,
bad cast). This is exactly how a latent bug in the original Q05a went unnoticed.

Statuses: `PASS`, `FAIL_SCHEMA`, `FAIL_DATA`, `FAIL_ORDER`, `ERROR_SQL`,
`ERROR_PYSPARK`, `MISSING`. The exit code is 1 on any FAIL or ERROR (and on MISSING
with `--fail-on-missing`).

## Conversion traps the suite catches

These were confirmed by running deliberately broken conversions through the harness:

| Trap | Where | How it shows up |
|------|-------|-----------------|
| SQL literal `60.0` is `DECIMAL(3,1)`; Python `60.0` is `DOUBLE` | Q01 | `FAIL_SCHEMA decimal(23,1) vs double` |
| `DataFrame.union()` is `UNION ALL`; SQL `UNION` needs `.distinct()` over the whole chain | Q07 | `FAIL_DATA`, 13 extra duplicate rows |
| `RANGE BETWEEN INTERVAL n DAYS` must become `orderBy(ts.cast("long")).rangeBetween(-n*86400, 0)` | Q08 | `FAIL_DATA` on `rolling_30d_usd` |
| `LEFT ANTI` turned into a plain left join | Q06b | `FAIL_DATA`, 1 vs 14 rows |
| `PIVOT ... IN ('2024-01' AS jan)` has no DataFrame alias list; columns must be renamed | Q03 | `FAIL_SCHEMA` on column names |
| `arr[0]` throws on an empty array under ANSI; use `get(arr, 0)` / `F.get()` | Q05a | `ERROR_*` with `INVALID_ARRAY_INDEX` |
| `aggregate()` accumulator widens `DECIMAL(18,2)` to `(19,2)` unless cast inside the lambda | Q12 | analysis error |
| Frame clause on a named window (`OVER (w ROWS ...)`) won't parse in Spark | Q12 | parse error |
| `UNPIVOT` aliases and `EXCLUDE NULLS` have no direct `DataFrame.unpivot()` equivalent | Q10a | `FAIL_DATA` on `quarter` values |
