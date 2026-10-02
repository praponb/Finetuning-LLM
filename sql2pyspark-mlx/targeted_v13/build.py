"""Build the v13 training data: uncorrelated scalar subqueries (raw_v13/verified.jsonl from
../targeted_v11/generate.py --counts v13) plus replay of the earlier data.

  python build.py     -> data/ (train / valid / heldout_meta.jsonl) and data_mix/ (what v13 trains on)

Why: v12's two remaining classic misses (gen01245, gen05869) are `x >= (SELECT MAX(x) FROM t WHERE ...)`: v12 compared
against a column of a separate aggregate DataFrame (MISSING_ATTRIBUTES); the classic data never had a WHERE inside
the scalar subquery. v12 also slipped on complex pipelines (22/32), so the complex_v9 replay is larger.
Empty results and any SQL already in an eval set or in targeted_v11 / targeted_v12 data are dropped, then (seeded
shuffle) HELDOUT pairs go to data/heldout_meta.jsonl, VALID to validation and TRAIN to training.
data_mix/ = those + --complex complex_v9 + --v12 targeted_v12 + --v11 targeted_v11 + --classic-or classic OR pairs +
--classic other classic pairs.
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

TRAIN = {"scalar_where": 2000}
HELDOUT = {"scalar_where": 150}
VALID = {"scalar_where": 75}
META_KEYS = ("id", "family", "features", "order_cols", "sql", "code")
EVAL_SETS = ["eval/holdout.jsonl", "eval/holdout_v4.jsonl", "eval/real_queries.jsonl", "eval/test_fixed.jsonl",
             "eval/complex_v8_test.jsonl", "sql2pyspark-mlx/classic_v10/data/inlist_valid_meta.jsonl",
             "sql2pyspark-mlx/targeted_v11/data/heldout_meta.jsonl", "sql2pyspark-mlx/targeted_v12/data/heldout_meta.jsonl"]
OR_RE = re.compile(r"\bOR\b", re.I)


def read_jsonl(p: Path) -> list:
    return [json.loads(line) for line in p.read_text(encoding="utf-8").splitlines() if line.strip()]


def chat_sql(line: str) -> str:
    return json.loads(line)["messages"][1]["content"]


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--classic", type=int, default=1500)
    ap.add_argument("--classic-or", type=int, default=500)
    ap.add_argument("--complex", type=int, default=4000)
    ap.add_argument("--v12", type=int, default=2000)
    ap.add_argument("--v11", type=int, default=1500)
    ap.add_argument("--replay-valid", type=int, default=300)
    ap.add_argument("--seed", type=int, default=13)
    args = ap.parse_args()
    rng = random.Random(args.seed)

    seen = {norm_sql(r["sql"]) for s in EVAL_SETS for r in read_jsonl(ROOT / s)}
    for d, n in [(d, n) for d in ("targeted_v11", "targeted_v12") for n in ("train", "valid")]:
        for line in (MLX / d / "data" / f"{n}.jsonl").read_text().splitlines():
            t = chat_sql(line)
            seen.add(norm_sql(t.split("```sql\n", 1)[1].rsplit("\n```", 1)[0]))
    rows = read_jsonl(HERE / "raw_v13" / "verified.jsonl")
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
    classic_or = [x for x in classic if OR_RE.search(chat_sql(x))]
    classic_plain = [x for x in classic if not OR_RE.search(chat_sql(x))]
    lines = [json.dumps(fmt(r), ensure_ascii=False) for r in train]
    lines += rng.sample(classic_or, args.classic_or)
    lines += rng.sample(classic_plain, args.classic)
    lines += rng.sample((MLX / "complex_v9" / "data" / "train.jsonl").read_text().splitlines(), args.complex)
    lines += rng.sample((MLX / "targeted_v12" / "data" / "train.jsonl").read_text().splitlines(), args.v12)
    lines += rng.sample((MLX / "targeted_v11" / "data" / "train.jsonl").read_text().splitlines(), args.v11)
    rng.shuffle(lines)
    vlines = [json.dumps(fmt(r), ensure_ascii=False) for r in valid]
    vlines += rng.sample((MLX / "targeted_v12" / "data_mix" / "valid.jsonl").read_text().splitlines(), args.replay_valid)
    rng.shuffle(vlines)
    mdir = HERE / "data_mix"
    mdir.mkdir(exist_ok=True)
    (mdir / "train.jsonl").write_text("\n".join(lines) + "\n")
    (mdir / "valid.jsonl").write_text("\n".join(vlines) + "\n")

    chars = [len(chat_sql(x)) + len(json.loads(x)["messages"][2]["content"]) for x in lines]
    stats["data_mix"] = {"train": len(lines), "new": len(train), "classic_or": args.classic_or,
                         "classic_plain": args.classic, "complex_v9": args.complex, "targeted_v12": args.v12,
                         "targeted_v11": args.v11,
                         "valid": len(vlines), "with_or": sum(bool(OR_RE.search(chat_sql(x))) for x in lines),
                         "mean_chars": round(sum(chars) / len(chars)), "max_chars": max(chars)}
    stats["train_features"] = dict(Counter(f for r in train for f in r["features"]).most_common())
    (out / "split_stats.json").write_text(json.dumps(stats, indent=2))
    print(json.dumps({k: v for k, v in stats.items() if k != "train_features"}, indent=2))


if __name__ == "__main__":
    main()
