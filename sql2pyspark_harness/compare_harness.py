#!/usr/bin/env python3
"""
sql2pyspark comparison harness
==============================

Runs every query in a tagged Spark SQL suite twice — once as SQL, once as the
converted PySpark — and checks that the two results are identical.

SQL file format
---------------
Each statement is preceded by an ID marker line:

    -- @id: setup:orders        <- ids starting with "setup:" are run first, not compared
    CREATE OR REPLACE TEMP VIEW orders AS ...;

    -- @id: Q01
    WITH ... SELECT ...;

Conversion format
-----------------
One Python file per query in the conversions directory, named after the ID
(e.g. conversions/Q01.py), exposing:

    def build(spark: SparkSession) -> DataFrame: ...

The setup views already exist when build() is called, so conversions read
them with spark.table("orders") etc.

What "identical" means
----------------------
1. Schema  — same column names (in order) and same data types.
             --loose-types: only names are checked; PySpark columns are cast to
             the SQL types before comparing data (a failed cast is a mismatch).
             --ignore-names: columns are matched by position only.
2. Data    — same multiset of rows (duplicates count). Doubles/floats are
             rounded to --float-places, maps are compared key-sorted, nested
             structs/arrays are compared recursively.
             --sort-arrays: arrays are also compared order-insensitively
             (use for collect_list/collect_set whose order Spark doesn't guarantee).
3. Order   — only with --ordered: rows must also appear in the same sequence.

Usage
-----
    python compare_harness.py --sql sql2pyspark_test_suite.sql --conversions conversions
    python compare_harness.py ... --only Q01,Q08 --show-diff 10
    python compare_harness.py ... --report report.json --junit junit.xml   # CI

Exit code is 1 if any query fails or errors (and, with --fail-on-missing, if
any query has no conversion), else 0.
"""
from __future__ import annotations

import argparse
import importlib.util
import json
import re
import sys
import time
import traceback
import xml.etree.ElementTree as ET
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Callable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

ID_RE = re.compile(r"^\s*--\s*@id:\s*(\S+)\s*$")

PASS, FAIL_SCHEMA, FAIL_DATA, FAIL_ORDER = "PASS", "FAIL_SCHEMA", "FAIL_DATA", "FAIL_ORDER"
ERROR_SQL, ERROR_PYSPARK, MISSING = "ERROR_SQL", "ERROR_PYSPARK", "MISSING"
FAILING = {FAIL_SCHEMA, FAIL_DATA, FAIL_ORDER, ERROR_SQL, ERROR_PYSPARK}


# --------------------------------------------------------------------------------------
# Suite loading
# --------------------------------------------------------------------------------------
@dataclass
class Statement:
    id: str
    sql: str

    @property
    def is_setup(self) -> bool:
        return self.id.startswith("setup:")


def _clean_sql(lines: list[str]) -> str:
    body = "\n".join(l for l in lines if not l.strip().startswith("--")).strip()
    return body[:-1].rstrip() if body.endswith(";") else body


def load_suite(path: str | Path) -> list[Statement]:
    """Split a tagged SQL file into statements keyed by their @id."""
    stmts: list[Statement] = []
    cur_id: str | None = None
    buf: list[str] = []
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        m = ID_RE.match(line)
        if m:
            if cur_id is not None:
                stmts.append(Statement(cur_id, _clean_sql(buf)))
            cur_id, buf = m.group(1), []
        elif cur_id is not None:
            buf.append(line)
    if cur_id is not None:
        stmts.append(Statement(cur_id, _clean_sql(buf)))

    seen: set[str] = set()
    for s in stmts:
        if s.id in seen:
            raise ValueError(f"duplicate @id: {s.id}")
        if not s.sql:
            raise ValueError(f"@id {s.id} has no SQL under it")
        seen.add(s.id)
    if not stmts:
        raise ValueError(f"no '-- @id:' markers found in {path}")
    return stmts


def run_setup(spark: SparkSession, stmts: list[Statement]) -> None:
    for s in stmts:
        if s.is_setup:
            spark.sql(s.sql)


def load_conversion(conv_dir: str | Path, qid: str) -> Callable[[SparkSession], DataFrame] | None:
    path = Path(conv_dir) / f"{qid}.py"
    if not path.exists():
        return None
    spec = importlib.util.spec_from_file_location(f"conv_{qid}", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)  # type: ignore[union-attr]
    if not hasattr(mod, "build"):
        raise AttributeError(f"{path} does not define build(spark)")
    return mod.build


# --------------------------------------------------------------------------------------
# Normalisation: turn every row into one canonical JSON string
# --------------------------------------------------------------------------------------
def _is_orderable(dt: T.DataType) -> bool:
    if isinstance(dt, T.MapType):
        return False
    if isinstance(dt, T.ArrayType):
        return _is_orderable(dt.elementType)
    if isinstance(dt, T.StructType):
        return all(_is_orderable(f.dataType) for f in dt.fields)
    return True


def _canon(c, dt: T.DataType, places: int, sort_arrays: bool):
    if isinstance(dt, (T.FloatType, T.DoubleType)):
        # round, and fold -0.0 into 0.0 so they compare equal
        return F.round(c.cast("double"), places) + F.lit(0.0)
    if isinstance(dt, T.ArrayType):
        el_t = dt.elementType
        out = F.transform(c, lambda x: _canon(x, el_t, places, sort_arrays))
        if sort_arrays and _is_orderable(el_t):
            out = F.array_sort(out)
        return out
    if isinstance(dt, T.StructType):
        if not dt.fields:
            return c
        rebuilt = F.struct(*[_canon(c[f.name], f.dataType, places, sort_arrays).alias(f.name)
                             for f in dt.fields])
        return F.when(c.isNull(), F.lit(None)).otherwise(rebuilt)
    if isinstance(dt, T.MapType):
        v_t = dt.valueType
        entries = F.map_entries(F.transform_values(c, lambda k, v: _canon(v, v_t, places, sort_arrays)))
        return F.array_sort(entries) if _is_orderable(v_t) else entries
    return c


def _unique_labels(names: list[str]) -> list[str]:
    seen: dict[str, int] = {}
    out = []
    for n in names:
        seen[n] = seen.get(n, 0) + 1
        out.append(n if seen[n] == 1 else f"{n}#{seen[n]}")
    return out


def fingerprint(df: DataFrame, places: int, sort_arrays: bool,
                labels: list[str] | None = None) -> DataFrame:
    """One string column 'row_json' holding a canonical JSON rendering of each row.

    Columns are taken by position and keyed by `labels` (default: df's own names),
    so both sides can be rendered with the same keys.
    """
    labels = _unique_labels(labels or df.columns)
    cols = [_canon(F.col(f"`{f.name}`"), f.dataType, places, sort_arrays).alias(lbl)
            for f, lbl in zip(df.schema.fields, labels)]
    return df.select(F.to_json(F.struct(*cols), {"ignoreNullFields": "false"}).alias("row_json"))


# --------------------------------------------------------------------------------------
# Comparison
# --------------------------------------------------------------------------------------
@dataclass
class Result:
    id: str
    status: str
    message: str = ""
    sql_rows: int | None = None
    pyspark_rows: int | None = None
    sql_seconds: float | None = None
    pyspark_seconds: float | None = None
    only_in_sql: list[str] = field(default_factory=list)
    only_in_pyspark: list[str] = field(default_factory=list)


@dataclass
class Options:
    float_places: int = 6
    sort_arrays: bool = False
    ordered: bool = False
    loose_types: bool = False
    ignore_names: bool = False
    show_diff: int = 5


def _schema_str(df: DataFrame) -> list[str]:
    return [f"{f.name}:{f.dataType.simpleString()}" for f in df.schema.fields]


def _timed_eval(df: DataFrame) -> tuple[int, float]:
    """Row count + seconds, forcing EVERY value to be computed.

    A plain df.count() lets Spark prune unused columns, so runtime errors inside
    projections (ANSI overflow, bad array index, failed cast...) would go unnoticed.
    Summing the length of a JSON rendering of each row makes all columns required.
    """
    t0 = time.perf_counter()
    row = (df.select(F.to_json(F.struct(*[F.col(f"`{c}`") for c in df.columns])).alias("j"))
             .agg(F.count(F.lit(1)).alias("n"), F.sum(F.length("j")).alias("chk"))
             .first())
    return int(row["n"]), round(time.perf_counter() - t0, 3)


def compare_one(spark: SparkSession, stmt: Statement, build, opt: Options) -> Result:
    r = Result(stmt.id, PASS)

    # --- SQL side -----------------------------------------------------------------
    try:
        sql_df = spark.sql(stmt.sql)
        r.sql_rows, r.sql_seconds = _timed_eval(sql_df)
    except Exception as e:  # noqa: BLE001
        r.status, r.message = ERROR_SQL, _short_err(e)
        return r

    if build is None:
        r.status, r.message = MISSING, "no conversion file"
        return r

    # --- PySpark side -------------------------------------------------------------
    try:
        py_df = build(spark)
        if not isinstance(py_df, DataFrame):
            raise TypeError(f"build() returned {type(py_df).__name__}, expected DataFrame")
        r.pyspark_rows, r.pyspark_seconds = _timed_eval(py_df)
    except Exception as e:  # noqa: BLE001
        r.status, r.message = ERROR_PYSPARK, _short_err(e)
        return r

    # --- Schema -------------------------------------------------------------------
    s_fields, p_fields = sql_df.schema.fields, py_df.schema.fields
    if len(s_fields) != len(p_fields):
        r.status = FAIL_SCHEMA
        r.message = (f"column count {len(s_fields)} (SQL) vs {len(p_fields)} (PySpark)\n"
                     f"  SQL:     {_schema_str(sql_df)}\n  PySpark: {_schema_str(py_df)}")
        return r

    problems = []
    for i, (s, p) in enumerate(zip(s_fields, p_fields)):
        if not opt.ignore_names and s.name != p.name:
            problems.append(f"col {i}: name '{s.name}' vs '{p.name}'")
        if not opt.loose_types and s.dataType != p.dataType:
            problems.append(f"col {i} '{s.name}': type {s.dataType.simpleString()} "
                            f"vs {p.dataType.simpleString()}")
    if problems:
        r.status, r.message = FAIL_SCHEMA, "\n".join("  " + x for x in problems)
        return r

    if opt.loose_types:
        py_df = py_df.select(*[F.col(f"`{p.name}`").cast(s.dataType).alias(s.name)
                               for s, p in zip(s_fields, p_fields)])

    # --- Data (multiset) ----------------------------------------------------------
    try:
        labels = sql_df.columns
        a = fingerprint(sql_df, opt.float_places, opt.sort_arrays, labels)
        b = fingerprint(py_df, opt.float_places, opt.sort_arrays, labels)
        only_a, only_b = a.exceptAll(b), b.exceptAll(a)
        n_a, n_b = only_a.count(), only_b.count()
    except Exception as e:  # noqa: BLE001  (e.g. a --loose-types cast failing under ANSI)
        r.status, r.message = FAIL_DATA, f"could not compare data: {_short_err(e)}"
        return r

    if n_a or n_b:
        r.status = FAIL_DATA
        r.message = (f"{n_a} row(s) only in SQL, {n_b} row(s) only in PySpark "
                     f"(row counts {r.sql_rows} vs {r.pyspark_rows})")
        r.only_in_sql = [x["row_json"] for x in only_a.limit(opt.show_diff).collect()]
        r.only_in_pyspark = [x["row_json"] for x in only_b.limit(opt.show_diff).collect()]
        return r

    # --- Order (optional) ---------------------------------------------------------
    if opt.ordered:
        la = [x["row_json"] for x in a.collect()]
        lb = [x["row_json"] for x in b.collect()]
        for i, (x, y) in enumerate(zip(la, lb)):
            if x != y:
                r.status = FAIL_ORDER
                r.message = f"same rows, different order; first difference at row {i}"
                r.only_in_sql, r.only_in_pyspark = [x], [y]
                return r

    return r


def _short_err(e: Exception) -> str:
    text = str(e).strip() or repr(e)
    lines = [l for l in text.splitlines() if l.strip()]
    head = lines[0] if lines else type(e).__name__
    return f"{type(e).__name__}: {head[:600]}"


# --------------------------------------------------------------------------------------
# Reporting
# --------------------------------------------------------------------------------------
_COLOR = {PASS: "32", MISSING: "33"}  # green / yellow, everything else red


def _paint(s: str, status: str, use_color: bool) -> str:
    if not use_color:
        return s
    return f"\033[{_COLOR.get(status, '31')}m{s}\033[0m"


def print_report(results: list[Result], use_color: bool) -> None:
    w = max(len(r.id) for r in results)
    print()
    for r in results:
        timing = ""
        if r.sql_seconds is not None and r.pyspark_seconds is not None:
            timing = f"  sql {r.sql_seconds:>6.2f}s  py {r.pyspark_seconds:>6.2f}s"
        rows = f"rows={r.sql_rows}" if r.sql_rows is not None else ""
        print(f"{r.id:<{w}}  {_paint(f'{r.status:<13}', r.status, use_color)} {rows:<10}{timing}")
        if r.status not in (PASS, MISSING) and r.message:
            for line in r.message.splitlines():
                print(f"{'':<{w}}    {line}")
        for label, rows_ in (("- only in SQL    ", r.only_in_sql), ("+ only in PySpark", r.only_in_pyspark)):
            for row in rows_:
                print(f"{'':<{w}}    {label} {row}")

    counts: dict[str, int] = {}
    for r in results:
        counts[r.status] = counts.get(r.status, 0) + 1
    print("\n" + "  ".join(f"{k}: {v}" for k, v in sorted(counts.items())) + f"  (total {len(results)})")


def write_json(results: list[Result], path: str) -> None:
    Path(path).write_text(json.dumps([asdict(r) for r in results], indent=2), encoding="utf-8")


def write_junit(results: list[Result], path: str) -> None:
    suite = ET.Element("testsuite", name="sql2pyspark", tests=str(len(results)),
                       failures=str(sum(r.status.startswith("FAIL") for r in results)),
                       errors=str(sum(r.status.startswith("ERROR") for r in results)),
                       skipped=str(sum(r.status == MISSING for r in results)))
    for r in results:
        tc = ET.SubElement(suite, "testcase", classname="sql2pyspark", name=r.id,
                           time=str((r.sql_seconds or 0) + (r.pyspark_seconds or 0)))
        detail = r.message + "".join(f"\n- {x}" for x in r.only_in_sql) \
                           + "".join(f"\n+ {x}" for x in r.only_in_pyspark)
        if r.status.startswith("FAIL"):
            ET.SubElement(tc, "failure", type=r.status, message=r.message.splitlines()[0] if r.message else "").text = detail
        elif r.status.startswith("ERROR"):
            ET.SubElement(tc, "error", type=r.status, message=r.message[:200]).text = detail
        elif r.status == MISSING:
            ET.SubElement(tc, "skipped", message="no conversion")
    ET.ElementTree(suite).write(path, encoding="utf-8", xml_declaration=True)


# --------------------------------------------------------------------------------------
# Entry point
# --------------------------------------------------------------------------------------
def make_spark(confs: list[str] | None = None) -> SparkSession:
    b = (SparkSession.builder.appName("sql2pyspark-harness")
         .master("local[*]")
         .config("spark.ui.enabled", "false")
         .config("spark.sql.shuffle.partitions", "4")
         .config("spark.sql.session.timeZone", "UTC"))
    for kv in confs or []:
        k, _, v = kv.partition("=")
        b = b.config(k, v)
    spark = b.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0],
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--sql", required=True, help="tagged Spark SQL suite file")
    ap.add_argument("--conversions", required=True, help="directory of <id>.py files with build(spark)")
    ap.add_argument("--only", help="comma-separated query IDs to run (default: all)")
    ap.add_argument("--float-places", type=int, default=6, help="decimal places for float/double equality")
    ap.add_argument("--sort-arrays", action="store_true", help="compare arrays order-insensitively")
    ap.add_argument("--ordered", action="store_true", help="also require identical row order")
    ap.add_argument("--loose-types", action="store_true", help="ignore type differences that cast cleanly")
    ap.add_argument("--ignore-names", action="store_true", help="match columns by position only")
    ap.add_argument("--show-diff", type=int, default=5, help="sample rows to print per side on mismatch")
    ap.add_argument("--fail-on-missing", action="store_true", help="treat missing conversions as failures")
    ap.add_argument("--report", help="write JSON results here")
    ap.add_argument("--junit", help="write JUnit XML here (for CI)")
    ap.add_argument("--conf", action="append", default=[], help="extra Spark conf key=value (repeatable)")
    ap.add_argument("--no-color", action="store_true")
    args = ap.parse_args(argv)

    stmts = load_suite(args.sql)
    queries = [s for s in stmts if not s.is_setup]
    if args.only:
        wanted = [x.strip() for x in args.only.split(",") if x.strip()]
        unknown = set(wanted) - {q.id for q in queries}
        if unknown:
            ap.error(f"unknown query id(s): {', '.join(sorted(unknown))}")
        queries = [q for q in queries if q.id in wanted]

    opt = Options(args.float_places, args.sort_arrays, args.ordered,
                  args.loose_types, args.ignore_names, args.show_diff)

    spark = make_spark(args.conf)
    print(f"Spark {spark.version} | {len(queries)} queries | conversions: {args.conversions}")
    run_setup(spark, stmts)

    results: list[Result] = []
    for q in queries:
        try:
            build = load_conversion(args.conversions, q.id)
        except Exception as e:  # noqa: BLE001  (syntax error etc. in the conversion file)
            results.append(Result(q.id, ERROR_PYSPARK, f"could not load conversion: {_short_err(e)}"))
            continue
        try:
            results.append(compare_one(spark, q, build, opt))
        except Exception as e:  # noqa: BLE001  harness bug safety net
            results.append(Result(q.id, ERROR_PYSPARK, "harness error: " + traceback.format_exc(limit=3)))

    print_report(results, use_color=not args.no_color and sys.stdout.isatty())
    if args.report:
        write_json(results, args.report)
    if args.junit:
        write_junit(results, args.junit)

    bad = any(r.status in FAILING for r in results)
    bad |= args.fail_on_missing and any(r.status == MISSING for r in results)
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
