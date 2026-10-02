"""Build the v11 training data from the verified targeted pairs (raw_v11/verified.jsonl, see generate.py).

  python build.py     -> data/ (train / valid / heldout_meta.jsonl) and data_mix/ (what v11 trains on)

Per family, pairs with an empty result on the sample tables are dropped, then (seeded shuffle) 50 go to
data/heldout_meta.jsonl (evaluation, never trained on), 50 to validation and TRAIN[family] to training (10,000 in
total). Any pair whose SQL is also in an eval set is dropped first. data_mix/ = the 10,000 new pairs + --replay pairs
from classic_v10/data_mix/train.jsonl (classic mirror, IN / NOT IN, plain classic, complex_v9 in their v10
proportions), so v11 keeps what v10 learned.
"""
from __future__ import annotations

import argparse
import json
import random
import sys
from collections import Counter
from pathlib import Path

HERE = Path(__file__).resolve().parent
MLX = HERE.parent
ROOT = MLX.parent
sys.path.insert(0, str(MLX))

from convert_sql_pyspark import DEFAULT_SYSTEM_PROMPT, format_sample, norm_sql, write_jsonl  # noqa: E402

TRAIN = {"exists_corr": 4000, "in_corr": 1000, "exists_multi": 1500, "exists_agg": 1500,
         "setop_proj": 1000, "lateral_filter": 1000}
META_KEYS = ("id", "family", "features", "order_cols", "sql", "code")
EVAL_SETS = ["eval/holdout.jsonl", "eval/holdout_v4.jsonl", "eval/real_queries.jsonl", "eval/test_fixed.jsonl",
             "eval/complex_v8_test.jsonl", "sql2pyspark-mlx/classic_v10/data/inlist_valid_meta.jsonl"]


def read_jsonl(p: Path) -> list:
    return [json.loads(line) for line in p.read_text(encoding="utf-8").splitlines() if line.strip()]


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--heldout", type=int, default=50)
    ap.add_argument("--valid", type=int, default=50)
    ap.add_argument("--replay", type=int, default=5000)
    ap.add_argument("--replay-valid", type=int, default=300)
    ap.add_argument("--seed", type=int, default=11)
    args = ap.parse_args()
    rng = random.Random(args.seed)

    eval_sql = set()
    for s in EVAL_SETS:
        for r in read_jsonl(ROOT / s):
            eval_sql.add(norm_sql(r["sql"]))
    rows = read_jsonl(HERE / "raw_v11" / "verified.jsonl")
    stats = {"verified": len(rows), "empty": sum(r["n_rows"] == 0 for r in rows)}
    rows = [r for r in rows if r["n_rows"] > 0]
    leaked = [r for r in rows if norm_sql(r["sql"]) in eval_sql]
    rows = [r for r in rows if norm_sql(r["sql"]) not in eval_sql]
    stats["in_eval_sets"] = len(leaked)

    train, valid, held = [], [], []
    per = {}
    for fam, n in TRAIN.items():
        fr = sorted((r for r in rows if r["family"] == fam), key=lambda r: r["id"])
        rng.shuffle(fr)
        need = args.heldout + args.valid + n
        if len(fr) < need:
            raise SystemExit(f"{fam}: only {len(fr)} non-empty verified pairs, need {need}")
        held += fr[: args.heldout]
        valid += fr[args.heldout: args.heldout + args.valid]
        train += fr[args.heldout + args.valid: need]
        per[fam] = {"available": len(fr), "train": n, "valid": args.valid, "heldout": args.heldout}
    stats["families"] = per

    out = HERE / "data"
    out.mkdir(exist_ok=True)
    rng.shuffle(train)
    fmt = lambda r: format_sample(r["sql"], r["code"], DEFAULT_SYSTEM_PROMPT)  # noqa: E731
    write_jsonl(out / "train.jsonl", [fmt(r) for r in train])
    write_jsonl(out / "valid.jsonl", [fmt(r) for r in valid])
    write_jsonl(out / "heldout_meta.jsonl", [{k: r[k] for k in META_KEYS} for r in held])

    v10 = MLX / "classic_v10" / "data_mix"
    lines = [json.dumps(fmt(r), ensure_ascii=False) for r in train]
    lines += rng.sample((v10 / "train.jsonl").read_text().splitlines(), args.replay)
    rng.shuffle(lines)
    vlines = [json.dumps(fmt(r), ensure_ascii=False) for r in valid]
    vlines += rng.sample((v10 / "valid.jsonl").read_text().splitlines(), args.replay_valid)
    rng.shuffle(vlines)
    mdir = HERE / "data_mix"
    mdir.mkdir(exist_ok=True)
    (mdir / "train.jsonl").write_text("\n".join(lines) + "\n")
    (mdir / "valid.jsonl").write_text("\n".join(vlines) + "\n")

    chars = [len(json.loads(x)["messages"][2]["content"]) + len(json.loads(x)["messages"][1]["content"]) for x in lines]
    stats["data_mix"] = {"train": len(lines), "new": len(train), "replay_v10_mix": args.replay, "valid": len(vlines),
                         "mean_chars": round(sum(chars) / len(chars)), "max_chars": max(chars)}
    stats["train_features"] = dict(Counter(f for r in train for f in r["features"]).most_common())
    (out / "split_stats.json").write_text(json.dumps(stats, indent=2))
    print(json.dumps({k: v for k, v in stats.items() if k != "train_features"}, indent=2))


if __name__ == "__main__":
    main()
