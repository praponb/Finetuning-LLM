"""Execute SQL / PySpark pairs on seeded sample tables and keep only pairs that agree.

For every pair the SQL is run with ``spark.sql`` against temp views and the
PySpark code is exec'd with ``df_<table>`` DataFrames bound; the code must
assign its output to ``result``. The two results must have the same column
names and the same rows (as a multiset; floats compared to 4 decimals). If the
pair has ORDER BY columns, the sequence of those columns must match as well
(ties may be ordered differently, the sort keys may not).

CLI:
    python verify_pairs.py --src raw_pairs --workers 4
        -> raw_pairs/verified.jsonl, raw_pairs/verify_report.json, raw_pairs/verify_failures.jsonl
Library:
    from verify_pairs import Checker
    Checker().check(sql, code, order_cols)
"""
from __future__ import annotations

import argparse
import datetime
import decimal
import json
import logging
import math
import multiprocessing as mp
import os
import sys
import time
import warnings
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
warnings.filterwarnings("ignore")


@dataclass
class Result:
    ok: bool
    status: str          # ok / sql_error / py_error / no_result / columns / rows / order
    detail: str = ""
    n_rows: int = 0


def norm(v):
    if v is None:
        return None
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float, decimal.Decimal)):
        f = float(v)
        return "nan" if math.isnan(f) else round(f, 4)
    if isinstance(v, (datetime.datetime, datetime.date)):
        return v.isoformat()
    if isinstance(v, (list, tuple)):
        return tuple(norm(x) for x in v)
    if isinstance(v, dict):
        return tuple(sorted((k, norm(x)) for k, x in v.items()))
    return v


def rows_of(df):
    return [tuple(norm(x) for x in r) for r in df.collect()]


class Checker:
    def __init__(self, seed: int = 7):
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F
        from pyspark.sql.window import Window

        from schemas import make_dataframes

        os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
        self.spark = (SparkSession.builder.master("local[1]").appName("verify-pairs")
                      .config("spark.ui.enabled", "false")
                      .config("spark.sql.shuffle.partitions", "1")
                      .config("spark.default.parallelism", "1")
                      .config("spark.driver.memory", "1g")
                      .config("spark.sql.session.timeZone", "UTC")
                      .config("spark.sql.adaptive.enabled", "false")
                      .config("spark.ui.showConsoleProgress", "false")
                      .getOrCreate())
        self.spark.sparkContext.setLogLevel("OFF")
        for name in ("SQLQueryContextLogger", "py4j", "pyspark"):
            logging.getLogger(name).setLevel(logging.CRITICAL + 1)
        self.frames = {k: v.cache() for k, v in make_dataframes(self.spark, seed).items()}
        for df in self.frames.values():
            df.count()
        self.base_ns = {"F": F, "Window": Window, "spark": self.spark}
        from pyspark.sql import DataFrame
        self.DataFrame = DataFrame

    def run_sql(self, sql: str):
        df = self.spark.sql(sql.strip().rstrip(";"))
        return df.columns, rows_of(df)

    def run_py(self, code: str):
        ns = dict(self.base_ns)
        ns.update(self.frames)
        exec(compile(code, "<pyspark>", "exec"), ns)
        res = ns.get("result")
        if not isinstance(res, self.DataFrame):
            raise LookupError("code did not assign a DataFrame to `result`")
        return res.columns, rows_of(res)

    def check(self, sql: str, code: str, order_cols=()) -> Result:
        try:
            s_cols, s_rows = self.run_sql(sql)
        except Exception as e:  # noqa: BLE001
            return Result(False, "sql_error", _short(e))
        return self.check_against(s_cols, s_rows, code, order_cols)

    def check_against(self, s_cols, s_rows, code, order_cols=()) -> Result:
        try:
            p_cols, p_rows = self.run_py(code)
        except LookupError as e:
            return Result(False, "no_result", _short(e))
        except Exception as e:  # noqa: BLE001
            return Result(False, "py_error", _short(e))
        return compare(s_cols, s_rows, p_cols, p_rows, order_cols)


def compare(s_cols, s_rows, p_cols, p_rows, order_cols=()) -> Result:
    if list(s_cols) != list(p_cols):
        return Result(False, "columns", f"sql={s_cols} py={p_cols}", len(s_rows))
    if Counter(map(repr, s_rows)) != Counter(map(repr, p_rows)):
        return Result(False, "rows", f"sql={len(s_rows)} rows, py={len(p_rows)} rows; "
                                     f"first diff: {_first_diff(s_rows, p_rows)}", len(s_rows))
    idx = [s_cols.index(c) for c in order_cols if c in s_cols]
    if idx:
        key = lambda rows: [tuple(r[i] for i in idx) for r in rows]  # noqa: E731
        if key(s_rows) != key(p_rows):
            return Result(False, "order", f"order by {list(order_cols)} differs", len(s_rows))
    return Result(True, "ok", "", len(s_rows))


def _first_diff(a, b):
    ca, cb = Counter(map(repr, a)), Counter(map(repr, b))
    only_a = list((ca - cb).elements())[:1]
    only_b = list((cb - ca).elements())[:1]
    return f"sql-only={only_a} py-only={only_b}"


def _short(e: Exception) -> str:
    msg = str(e).strip().splitlines()
    first = next((m for m in msg if m.strip()), type(e).__name__)
    return f"{type(e).__name__}: {first[:300]}"


# ---------------------------------------------------------------------------
# CLI (multiprocess)
# ---------------------------------------------------------------------------
_checker: Checker | None = None


def _init_worker():
    global _checker
    _checker = Checker()


def _work(rec):
    assert _checker is not None, "worker not initialised"
    r = _checker.check(rec["sql"], rec["code"], rec.get("order_cols") or ())
    return rec, asdict(r)


def load_pairs(src: Path):
    meta = {}
    for mf in sorted(src.glob("meta_*.jsonl")):
        for line in mf.read_text(encoding="utf-8").splitlines():
            if line.strip():
                m = json.loads(line)
                meta[m["id"]] = m
    recs = []
    for sql_file in sorted(src.glob("*.sql")):
        py_file = sql_file.with_suffix(".py")
        if not py_file.exists():
            continue
        m = meta.get(sql_file.stem, {"id": sql_file.stem, "family": "unknown", "features": [], "order_cols": []})
        recs.append({**m, "sql": sql_file.read_text(encoding="utf-8").strip(),
                     "code": py_file.read_text(encoding="utf-8").strip()})
    return recs


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--src", default="raw_pairs")
    ap.add_argument("--workers", type=int, default=max(1, min(6, (os.cpu_count() or 2) // 2)))
    ap.add_argument("--limit", type=int, default=0, help="only check the first N pairs (debugging)")
    args = ap.parse_args()
    src = Path(args.src)
    recs = load_pairs(src)
    if args.limit:
        recs = recs[: args.limit]
    print(f"checking {len(recs)} pairs with {args.workers} workers ...", flush=True)
    t0 = time.time()
    ok, fails = [], []
    by_status, by_family = Counter(), defaultdict(Counter)
    ctx = mp.get_context("spawn")
    with ctx.Pool(args.workers, initializer=_init_worker) as pool:
        for i, (rec, res) in enumerate(pool.imap_unordered(_work, recs, chunksize=8), 1):
            by_status[res["status"]] += 1
            by_family[rec.get("family", "unknown")][res["status"]] += 1
            if res["ok"]:
                ok.append({**rec, "n_rows": res["n_rows"]})
            else:
                fails.append({**rec, **res})
            if i % 500 == 0:
                print(f"  {i}/{len(recs)}  ok={len(ok)}  ({time.time() - t0:.0f}s)", flush=True)
    ok.sort(key=lambda r: r["id"])
    with open(src / "verified.jsonl", "w", encoding="utf-8") as f:
        for r in ok:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    with open(src / "verify_failures.jsonl", "w", encoding="utf-8") as f:
        for r in sorted(fails, key=lambda r: r["id"]):
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    empty = sum(1 for r in ok if r["n_rows"] == 0)
    report = {
        "total": len(recs), "ok": len(ok), "pass_rate": round(len(ok) / max(1, len(recs)), 4),
        "ok_but_empty_result": empty, "by_status": dict(by_status),
        "by_family": {k: dict(v) for k, v in sorted(by_family.items())},
        "seconds": round(time.time() - t0, 1),
    }
    (src / "verify_report.json").write_text(json.dumps(report, indent=2))
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
