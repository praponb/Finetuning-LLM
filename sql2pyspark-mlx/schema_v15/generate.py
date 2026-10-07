"""v15 pairs: the existing query families on the synthetic catalog (synth_catalog.py), so the forms stay familiar while
every table / column / struct-field name is new.

Groups (id prefix):
  classic (c15)  generate_dataset.py families: select, aggregation, joins, windows, top-N, CTEs, subqueries, set ops
  adv     (a15)  advanced.py flat-table families: multi-stage pipelines, dedup, sessionize, reshape, advanced windows
  nested  (n15)  nested_builder.py: array-of-struct lambdas, struct fields, LATERAL VIEW EXPLODE
  subq    (q15)  ../targeted_v11/generate.py: correlated [NOT] EXISTS / IN, two subqueries, GROUP BY after a subquery,
                 set operations, OR / mixed AND-OR (or_where, plus OR inside the others at rate 0.3)
  scalar  (s15)  ../targeted_v14/generate.py: uncorrelated / correlated / child-table / mixed scalar subqueries

  python generate.py --split train   --out raw_v15        (the 30 training domains)
  python generate.py --split heldout --out raw_v15_shift  (the 5 HELDOUT_DOMAINS: the schema-shift eval set)
"""
from __future__ import annotations

import argparse
import importlib.util
import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
MLX = HERE.parent
sys.path[:0] = [str(HERE), str(MLX)]

import patch  # noqa: E402
import synth_catalog as sc  # noqa: E402

COUNTS = {"train": {"classic": 5600, "adv": 1500, "nested": 1400, "subq": 2800, "scalar": 2700},
          "heldout": {"classic": 130, "adv": 35, "nested": 45, "subq": 65, "scalar": 65}}
PREFIX = {"classic": "c15", "adv": "a15", "nested": "n15", "subq": "q15", "scalar": "s15"}


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--split", choices=["train", "heldout"], default="train")
    ap.add_argument("--out", default="raw_v15")
    ap.add_argument("--seed", type=int, default=15)
    ap.add_argument("--scale", type=float, default=1.0)
    ap.add_argument("--groups", default=None, help="comma list (default: all)")
    args = ap.parse_args()
    held = args.split == "heldout"
    doms = {d: v for d, v in sc.CATALOG.items() if (d in sc.HELDOUT_DOMAINS) == held}
    patch.install([t for v in doms.values() for t in v["tables"]])

    import generate_dataset as gd
    import advanced
    import nested_builder as nb
    from sqlspec import Ctx, Style, render_code
    g11 = load("gen_v11", MLX / "targeted_v11" / "generate.py")
    g14 = load("gen_v14", MLX / "targeted_v14" / "generate.py")
    g11.OR_RATE = 0.3
    for r in (gd.rng, g11.rng, g14.rng, nb.rng):
        r.seed(args.seed + (1 if held else 0))

    adv_names = {f.__name__ for f, _ in advanced.FAMILIES}
    classic = [(f, w) for f, w in gd.FAMILIES if f.__name__ not in adv_names]
    adv = [(f, w) for f, w in advanced.FAMILIES if f.__name__ != "fam_adv_nested"]

    sys.path.insert(0, str(MLX / "classic_v10"))
    from mirror import mirror_code     # inline window specs ("mirror" style, as v10+); the generators hoist them

    def run_sqlspec(fams):
        fam = gd.rng.choices([f for f, _ in fams], [w for _, w in fams])[0]
        st, ctx = Style(gd.rng), Ctx()
        sql, defs, result, feats, order_cols = fam(st, ctx)
        sql = sql.strip() + (";" if st.semicolon else "")
        code = mirror_code(sql, render_code(ctx, defs, result) + "\n")
        if code is None:
            raise ValueError("mirror")
        return fam.__name__[4:], sql, code, set(feats), order_cols

    sub_fams = [("exists_corr", g11.build_exists_corr, 0.25), ("in_corr", g11.build_in_corr, 0.1),
                ("exists_multi", g11.build_exists_multi, 0.15), ("exists_agg", g11.build_exists_agg, 0.15),
                ("setop_proj", g11.build_setop, 0.1), ("or_where", g11.build_or_where, 0.25)]
    sc_fams = [("scalar_uncorr", g14.build_uncorr), ("scalar_corr_win", g14.build_corr_win),
               ("scalar_corr_join", g14.build_corr_join), ("scalar_mixed", g14.build_mixed)]
    dom_list = list(doms.values())

    def one(group):
        if group == "classic":
            return run_sqlspec(classic)
        if group == "adv":
            return run_sqlspec(adv)
        if group == "nested":
            sql, code, feats, oc = nb.build(nb.rng.choice(dom_list))
            return "nested", sql, code, feats, oc
        if group == "subq":
            name, fn, _ = g11.rng.choices(sub_fams, [w for *_, w in sub_fams])[0]
            sql, code, feats, oc = fn()
            return name, sql, code, set(feats), oc
        name, fn = g14.rng.choice(sc_fams)
        sql, code, feats, oc = fn()
        return name, sql, code, set(feats), oc

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    seen, metas = set(), []
    for group in (args.groups or ",".join(COUNTS[args.split])).split(","):
        n, made, tries, errs = int(COUNTS[args.split][group] * args.scale), 0, 0, {}
        while made < n and tries < n * 60:
            tries += 1
            try:
                fam, sql, code, feats, order_cols = one(group)
            except Exception as e:       # Skip, and the few branches that name a classic table (KeyError)
                errs[type(e).__name__] = errs.get(type(e).__name__, 0) + 1
                continue
            key = " ".join(sql.lower().rstrip(";").split())
            if key in seen:
                continue
            seen.add(key)
            made += 1
            pid = f"{PREFIX[group]}{made:05d}"
            (out / f"{pid}.sql").write_text(sql + "\n")
            (out / f"{pid}.py").write_text(code)
            metas.append({"id": pid, "family": fam, "group": group, "features": sorted(feats | {"schema_v15"}),
                          "order_cols": order_cols})
        print(f"{group}: {made} pairs ({tries} tries; skipped {errs})")
    with open(out / "meta_v15.jsonl", "w") as f:
        for m in metas:
            f.write(json.dumps(m) + "\n")
    print(f"wrote {len(metas)} pairs to {out}")


if __name__ == "__main__":
    main()
