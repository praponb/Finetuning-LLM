"""Build the v14 training data: correlated + uncorrelated scalar subqueries side by side (raw_v14/verified.jsonl from
generate.py) plus a large replay of the earlier data.

  python build.py     -> data/ (train / valid / heldout_meta.jsonl) and data_mix/ (what v14 trains on)

Why: v13 trained on uncorrelated scalar subqueries only (2,000 pairs, small classic replay) and then wrote correlated
ones as a separate aggregate value too (classic subquery family 6 / 17, hold-out 52). v14 has 10,000 new pairs in which
both forms appear, replays every classic scalar-subquery pair and puts the classic ones in validation as well.
Empty results and any SQL already in an eval set, in the classic test split or in targeted_v11 / v12 / v13 data are
dropped, then (seeded shuffle) HELDOUT pairs per family go to data/heldout_meta.jsonl, VALID to validation and TRAIN
to training.
data_mix/ = those + every classic scalar-subquery pair + --classic other classic pairs + --classic-or classic OR pairs
+ --complex complex_v9 + --v12 targeted_v12 + --v11 targeted_v11.
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

TRAIN = {"scalar_uncorr": 2500, "scalar_corr_win": 3000, "scalar_corr_join": 2500, "scalar_mixed": 2000}
HELDOUT = {f: 50 for f in TRAIN}
VALID = {f: 50 for f in TRAIN}
META_KEYS = ("id", "family", "features", "order_cols", "sql", "code")
EVAL_SETS = ["eval/holdout.jsonl", "eval/holdout_v4.jsonl", "eval/real_queries.jsonl", "eval/test_fixed.jsonl",
             "eval/complex_v8_test.jsonl", "sql2pyspark-mlx/classic_v10/data/inlist_valid_meta.jsonl",
             "sql2pyspark-mlx/classic_v10/data/test_meta.jsonl",
             "sql2pyspark-mlx/targeted_v11/data/heldout_meta.jsonl", "sql2pyspark-mlx/targeted_v12/data/heldout_meta.jsonl",
             "sql2pyspark-mlx/targeted_v13/data/heldout_meta.jsonl"]
OR_RE = re.compile(r"\bOR\b", re.I)
SCALAR_RE = re.compile(r"\(\s*SELECT\s+(AVG|MAX|MIN|SUM|COUNT)\(", re.I)


def read_jsonl(p: Path) -> list:
    return [json.loads(line) for line in p.read_text(encoding="utf-8").splitlines() if line.strip()]


def chat_sql(line: str) -> str:
    return json.loads(line)["messages"][1]["content"]


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--classic", type=int, default=5000)
    ap.add_argument("--classic-or", type=int, default=500)
    ap.add_argument("--complex", type=int, default=4500)
    ap.add_argument("--v12", type=int, default=2500)
    ap.add_argument("--v11", type=int, default=1800)
    ap.add_argument("--replay-valid", type=int, default=150)
    ap.add_argument("--seed", type=int, default=14)
    args = ap.parse_args()
    rng = random.Random(args.seed)

    seen = {norm_sql(r["sql"]) for s in EVAL_SETS for r in read_jsonl(ROOT / s)}
    for d, n in [(d, n) for d in ("targeted_v11", "targeted_v12", "targeted_v13") for n in ("train", "valid")]:
        for line in (MLX / d / "data" / f"{n}.jsonl").read_text().splitlines():
            t = chat_sql(line)
            seen.add(norm_sql(t.split("```sql\n", 1)[1].rsplit("\n```", 1)[0]))
    rows = read_jsonl(HERE / "raw_v14" / "verified.jsonl")
    stats = {"verified": len(rows), "empty": sum(r["n_rows"] == 0 for r in rows)}
    rows = [r for r in rows if r["n_rows"] > 0]
    stats["dropped_seen_sql"] = sum(norm_sql(r["sql"]) in seen for r in rows)
    rows = [r for r in rows if norm_sql(r["sql"]) not in seen]

    train, valid, held, per = [], [], [], {}
    for fam, n in TRAIN.items():
        fr = sorted((r for r in rows if r["family"] == fam), key=lambda r: r["id"])
        rng.shuffle(fr)
        need = HELDOUT[fam] + VALID[fam] + n
        if len(fr) < need:
            raise SystemExit(f"{fam}: only {len(fr)} non-empty verified pairs, need {need}")
        held += fr[: HELDOUT[fam]]
        valid += fr[HELDOUT[fam]: HELDOUT[fam] + VALID[fam]]
        train += fr[HELDOUT[fam] + VALID[fam]: need]
        per[fam] = {"available": len(fr), "train": n, "valid": VALID[fam], "heldout": HELDOUT[fam]}
    stats["families"] = per

    out = HERE / "data"
    out.mkdir(exist_ok=True)
    rng.shuffle(train)
    fmt = lambda r: format_sample(r["sql"], r["code"], DEFAULT_SYSTEM_PROMPT)  # noqa: E731
    write_jsonl(out / "train.jsonl", [fmt(r) for r in train])
    write_jsonl(out / "valid.jsonl", [fmt(r) for r in valid])
    write_jsonl(out / "heldout_meta.jsonl", [{k: r[k] for k in META_KEYS} for r in held])

    classic = (MLX / "classic_v10" / "data" / "train.jsonl").read_text().splitlines()
    classic_scalar = [x for x in classic if SCALAR_RE.search(chat_sql(x))]
    rest = [x for x in classic if not SCALAR_RE.search(chat_sql(x))]
    classic_or = [x for x in rest if OR_RE.search(chat_sql(x))]
    classic_plain = [x for x in rest if not OR_RE.search(chat_sql(x))]
    lines = [json.dumps(fmt(r), ensure_ascii=False) for r in train]
    lines += classic_scalar
    lines += rng.sample(classic_or, args.classic_or)
    lines += rng.sample(classic_plain, args.classic)
    lines += rng.sample((MLX / "complex_v9" / "data" / "train.jsonl").read_text().splitlines(), args.complex)
    lines += rng.sample((MLX / "targeted_v12" / "data" / "train.jsonl").read_text().splitlines(), args.v12)
    lines += rng.sample((MLX / "targeted_v11" / "data" / "train.jsonl").read_text().splitlines(), args.v11)
    rng.shuffle(lines)
    # validation: the new forms, every classic scalar-subquery validation pair (the forms v13 broke) and a replay
    cvalid = [x for x in (MLX / "classic_v10" / "data" / "valid.jsonl").read_text().splitlines()
              if SCALAR_RE.search(chat_sql(x))]
    vlines = [json.dumps(fmt(r), ensure_ascii=False) for r in valid] + cvalid
    vlines += rng.sample((MLX / "targeted_v12" / "data_mix" / "valid.jsonl").read_text().splitlines(), args.replay_valid)
    rng.shuffle(vlines)
    mdir = HERE / "data_mix"
    mdir.mkdir(exist_ok=True)
    (mdir / "train.jsonl").write_text("\n".join(lines) + "\n")
    (mdir / "valid.jsonl").write_text("\n".join(vlines) + "\n")

    chars = [len(chat_sql(x)) + len(json.loads(x)["messages"][2]["content"]) for x in lines]
    stats["data_mix"] = {"train": len(lines), "new": len(train), "classic_scalar": len(classic_scalar),
                         "classic_or": args.classic_or, "classic_plain": args.classic, "complex_v9": args.complex,
                         "targeted_v12": args.v12, "targeted_v11": args.v11,
                         "valid": len(vlines), "valid_classic_scalar": len(cvalid),
                         "with_scalar_subquery": sum(bool(SCALAR_RE.search(chat_sql(x))) for x in lines),
                         "mean_chars": round(sum(chars) / len(chars)), "max_chars": max(chars)}
    stats["train_features"] = dict(Counter(f for r in train for f in r["features"]).most_common())
    (out / "split_stats.json").write_text(json.dumps(stats, indent=2))
    print(json.dumps({k: v for k, v in stats.items() if k != "train_features"}, indent=2))


if __name__ == "__main__":
    main()
