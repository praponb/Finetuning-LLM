"""Build the v15 training data: the existing query forms on 30 synthetic domains (raw_v15/verified.jsonl) plus a replay
of the earlier data, and the schema-shift eval set on 5 domains that training never sees (raw_v15_shift).

  python build.py     -> data/ (train / valid / schema_shift_meta.jsonl) and data_mix/ (what v15 trains on)

Why: v13 and v14 copied names from the 42 training tables into unseen schemas (`df_patents`, `apppt_date`,
`num_stores`, `ss.restock`). These pairs keep the forms and change every name.
Empty results and any SQL already in an eval set are dropped; per group TRAIN pairs go to training and VALID to
validation (seeded shuffle). The schema-shift set takes SHIFT pairs per group from the held-out domains.
data_mix/ = the new pairs + --v14 targeted_v14 + every classic scalar-subquery pair + --classic other classic +
--classic-or classic OR + --complex complex_v9 + --v12 targeted_v12 + --v11 targeted_v11.
"""
from __future__ import annotations

import argparse
import json
import random
import re
import sys
from collections import Counter
from pathlib import Path

HERE = Path(__file__).resolve().parent
MLX = HERE.parent
ROOT = MLX.parent
sys.path.insert(0, str(MLX))

from convert_sql_pyspark import DEFAULT_SYSTEM_PROMPT, format_sample, norm_sql, write_jsonl  # noqa: E402

TRAIN = {"classic": 4000, "adv": 1000, "nested": 1000, "subq": 2000, "scalar": 2000}
VALID = {g: 40 for g in TRAIN}
SHIFT = {"classic": 90, "adv": 20, "nested": 30, "subq": 30, "scalar": 30}
META_KEYS = ("id", "family", "group", "features", "order_cols", "sql", "code")
EVAL_SETS = ["eval/holdout.jsonl", "eval/holdout_v4.jsonl", "eval/real_queries.jsonl", "eval/test_fixed.jsonl",
             "eval/complex_v8_test.jsonl", "sql2pyspark-mlx/classic_v10/data/inlist_valid_meta.jsonl"]
OR_RE = re.compile(r"\bOR\b", re.I)
SCALAR_RE = re.compile(r"\(\s*SELECT\s+(AVG|MAX|MIN|SUM|COUNT)\(", re.I)


def read_jsonl(p: Path) -> list:
    return [json.loads(line) for line in p.read_text(encoding="utf-8").splitlines() if line.strip()]


def chat_sql(line: str) -> str:
    return json.loads(line)["messages"][1]["content"]


def pick(rows, counts, rng, what):
    out, per = {}, {}
    for g, n in counts.items():
        gr = sorted((r for r in rows if r["group"] == g), key=lambda r: r["id"])
        rng.shuffle(gr)
        if len(gr) < n:
            raise SystemExit(f"{what} {g}: only {len(gr)} usable pairs, need {n}")
        out[g] = gr
        per[g] = len(gr)
    return out, per


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--v14", type=int, default=3000)
    ap.add_argument("--classic", type=int, default=4800)
    ap.add_argument("--classic-or", type=int, default=500)
    ap.add_argument("--complex", type=int, default=3500)
    ap.add_argument("--v12", type=int, default=1500)
    ap.add_argument("--v11", type=int, default=1000)
    ap.add_argument("--replay-valid", type=int, default=100)
    ap.add_argument("--seed", type=int, default=15)
    args = ap.parse_args()
    rng = random.Random(args.seed)
    seen = {norm_sql(r["sql"]) for s in EVAL_SETS for r in read_jsonl(ROOT / s)}
    stats = {}

    def usable(src):
        rows = read_jsonl(HERE / src / "verified.jsonl")
        st = {"verified": len(rows), "empty": sum(r["n_rows"] == 0 for r in rows)}
        rows = [r for r in rows if r["n_rows"] > 0 and norm_sql(r["sql"]) not in seen]
        st["usable"] = len(rows)
        return rows, st

    rows, stats["raw_v15"] = usable("raw_v15")
    groups, stats["available"] = pick(rows, {g: TRAIN[g] + VALID[g] for g in TRAIN}, rng, "train")
    train = [r for g, n in TRAIN.items() for r in groups[g][VALID[g]: VALID[g] + n]]
    valid = [r for g, n in VALID.items() for r in groups[g][:n]]
    srows, stats["raw_v15_shift"] = usable("raw_v15_shift")
    sgroups, stats["shift_available"] = pick(srows, SHIFT, rng, "schema-shift")
    shift = [r for g, n in SHIFT.items() for r in sgroups[g][:n]]

    out = HERE / "data"
    out.mkdir(exist_ok=True)
    rng.shuffle(train)
    fmt = lambda r: format_sample(r["sql"], r["code"], DEFAULT_SYSTEM_PROMPT)  # noqa: E731
    write_jsonl(out / "train.jsonl", [fmt(r) for r in train])
    write_jsonl(out / "valid.jsonl", [fmt(r) for r in valid])
    write_jsonl(out / "schema_shift_meta.jsonl", [{k: r[k] for k in META_KEYS} for r in shift])

    classic = (MLX / "classic_v10" / "data" / "train.jsonl").read_text().splitlines()
    classic_scalar = [x for x in classic if SCALAR_RE.search(chat_sql(x))]
    rest = [x for x in classic if not SCALAR_RE.search(chat_sql(x))]
    classic_or = [x for x in rest if OR_RE.search(chat_sql(x))]
    classic_plain = [x for x in rest if not OR_RE.search(chat_sql(x))]
    lines = [json.dumps(fmt(r), ensure_ascii=False) for r in train]
    lines += rng.sample((MLX / "targeted_v14" / "data" / "train.jsonl").read_text().splitlines(), args.v14)
    lines += classic_scalar
    lines += rng.sample(classic_or, args.classic_or)
    lines += rng.sample(classic_plain, args.classic)
    lines += rng.sample((MLX / "complex_v9" / "data" / "train.jsonl").read_text().splitlines(), args.complex)
    lines += rng.sample((MLX / "targeted_v12" / "data" / "train.jsonl").read_text().splitlines(), args.v12)
    lines += rng.sample((MLX / "targeted_v11" / "data" / "train.jsonl").read_text().splitlines(), args.v11)
    rng.shuffle(lines)
    cvalid = [x for x in (MLX / "classic_v10" / "data" / "valid.jsonl").read_text().splitlines()
              if SCALAR_RE.search(chat_sql(x))]
    vlines = [json.dumps(fmt(r), ensure_ascii=False) for r in valid] + cvalid
    vlines += rng.sample((MLX / "targeted_v14" / "data_mix" / "valid.jsonl").read_text().splitlines(), args.replay_valid)
    rng.shuffle(vlines)
    mdir = HERE / "data_mix"
    mdir.mkdir(exist_ok=True)
    (mdir / "train.jsonl").write_text("\n".join(lines) + "\n")
    (mdir / "valid.jsonl").write_text("\n".join(vlines) + "\n")

    chars = [len(chat_sql(x)) + len(json.loads(x)["messages"][2]["content"]) for x in lines]
    stats["data_mix"] = {"train": len(lines), "new": len(train), "targeted_v14": args.v14,
                         "classic_scalar": len(classic_scalar), "classic_or": args.classic_or,
                         "classic_plain": args.classic, "complex_v9": args.complex, "targeted_v12": args.v12,
                         "targeted_v11": args.v11, "valid": len(vlines), "schema_shift": len(shift),
                         "mean_chars": round(sum(chars) / len(chars)), "max_chars": max(chars)}
    stats["train_groups"] = dict(Counter(r["group"] for r in train))
    stats["train_families"] = dict(Counter(r["family"] for r in train).most_common())
    (out / "split_stats.json").write_text(json.dumps(stats, indent=2))
    print(json.dumps({k: v for k, v in stats.items() if k != "train_families"}, indent=2))


if __name__ == "__main__":
    main()
