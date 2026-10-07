"""Nested-data pairs on the synthetic catalog: one ARRAY<STRUCT<...>> column and one STRUCT column per domain, with
field names the model has not seen (v4 hold-out slips: `name` for `stop_name`, `num_stores` for `num_stops`).

Shapes (the code style follows advanced.py):
  select    SIZE / TRANSFORM / FILTER / EXISTS / FORALL / AGGREGATE / ARRAY_MAX over the array, struct fields
            -> .select(..., F.size("arr").alias(...), F.transform("arr", lambda v: v["f"]).alias(...), F.col("st.f"))
  explode   LATERAL VIEW [OUTER] EXPLODE(arr) x AS e, then e.<field> (optionally filtered)
            -> .withColumn("e", F.explode("arr")).select(..., F.col("e.f"))
"""
from __future__ import annotations

import json
import random

rng = random.Random(15)


class Skip(Exception):
    pass


def q(s):
    return json.dumps(s)


class St:
    def __init__(self):
        self.up = rng.random() < 0.75
        self.ml = rng.random() < 0.5
        self.semi = rng.random() < 0.5

    def k(self, s):
        return s if self.up else s.lower()


def num_fields(fields):
    return [f for f in fields if f[1] in ("int", "double")]


def cat_fields(fields):
    return [f for f in fields if f[1] == "cat"]


def threshold(f):
    lo, hi = f[2]
    v = lo + (hi - lo) * rng.uniform(0.3, 0.7)
    return int(round(v)) if f[1] == "int" or abs(v) >= 10 else round(v, 1)


def items_for(st, arr, fields, sfields, stc):
    """candidate (sql, py, out_name, feature) items over the array / struct"""
    k = st.k
    v = rng.choice(["x", "e", "it", arr[0], arr[:2]])
    nf, cf = num_fields(fields), cat_fields(fields)
    out = [(f"{k('SIZE')}({arr})", f'F.size({q(arr)})', rng.choice([f"num_{arr}", f"{arr}_cnt", f"n_{arr}"]), "array_size")]
    if cf:
        f = rng.choice(cf)
        out.append((f"{k('TRANSFORM')}({arr}, {v} -> {v}.{f[0]})", f'F.transform({q(arr)}, lambda {v}: {v}[{q(f[0])}])',
                    rng.choice([f"{f[0]}_list", f"all_{f[0]}s", f"{f[0]}_values"]), "lambda_transform"))
        val = rng.choice(f[2])
        out.append((f"{k('EXISTS')}({arr}, {v} -> {v}.{f[0]} = '{val}')", f'F.exists({q(arr)}, lambda {v}: {v}[{q(f[0])}] == {q(val)})',
                    f"has_{val.lower().replace('-', '_')}_{f[0]}", "lambda_exists"))
    if nf:
        f = rng.choice(nf)
        t = threshold(f)
        out.append((f"{k('SIZE')}({k('FILTER')}({arr}, {v} -> {v}.{f[0]} >= {t}))",
                    f'F.size(F.filter({q(arr)}, lambda {v}: {v}[{q(f[0])}] >= {t}))',
                    rng.choice([f"{f[0]}_over_{t}".replace(".", "_"), f"high_{f[0]}_cnt"]), "lambda_filter"))
        out.append((f"{k('FORALL')}({arr}, {v} -> {v}.{f[0]} <= {t})", f'F.forall({q(arr)}, lambda {v}: {v}[{q(f[0])}] <= {t})',
                    f"all_{f[0]}_le_{t}".replace(".", "_"), "lambda_forall"))
        f2 = rng.choice(nf)
        zero = rng.choice(["0D", "CAST(0 AS DOUBLE)", "0.0D"]) if st.up else rng.choice(["0D", "cast(0 as double)"])
        out.append((f"{k('AGGREGATE')}({arr}, {zero}, (acc, {v}) -> acc + {v}.{f2[0]}, acc -> {k('ROUND')}(acc, 2))",
                    f'F.aggregate({q(arr)}, F.lit(0.0), lambda acc, {v}: acc + {v}[{q(f2[0])}], lambda acc: F.round(acc, 2))',
                    rng.choice([f"total_{f2[0]}", f"{f2[0]}_sum", f"sum_of_{f2[0]}"]), "lambda_aggregate"))
        out.append((f"{k('ARRAY_MAX')}({k('TRANSFORM')}({arr}, {v} -> {v}.{f2[0]}))",
                    f'F.array_max(F.transform({q(arr)}, lambda {v}: {v}[{q(f2[0])}]))',
                    rng.choice([f"max_{f2[0]}", f"peak_{f2[0]}"]), "lambda_transform"))
        if cf:
            c = rng.choice(cf)
            out.append((f"{k('TRANSFORM')}({k('FILTER')}({arr}, {v} -> {v}.{f[0]} >= {t}), {v} -> {v}.{c[0]})",
                        f'F.transform(F.filter({q(arr)}, lambda {v}: {v}[{q(f[0])}] >= {t}), lambda {v}: {v}[{q(c[0])}])',
                        rng.choice([f"{c[0]}_where_{f[0]}_high", f"busy_{c[0]}s"]), "lambda_filter_transform"))
    for f in sfields:
        alias = rng.choice([f"{stc}_{f[0]}", f[0]])
        out.append((f"{stc}.{f[0]}" + (f" {k('AS')} {alias}" if alias != f[0] else ""), f'F.col({q(stc + "." + f[0])})' +
                    (f'.alias({q(alias)})' if alias != f[0] else ""), alias, "struct_field"))
    return out


def build(dom: dict) -> tuple:
    st = St()
    k = st.k
    t, arr, stc = dom["nested"], dom["arr"], dom["st"]
    fields, sfields = dom["arr_fields"], dom["st_fields"]
    plain = [c for c in t.columns if c.kind not in ("array_struct", "struct", "id")]
    keep = ["id"] + [c.name for c in rng.sample(plain, min(len(plain), rng.choice([0, 1, 1, 2])))]
    feats = {"nested_v15"}
    order_cols = []
    if rng.random() < 0.3:      # explode
        e = rng.choice(["e", "elem", arr.rstrip("s") if arr.rstrip("s") != arr else "item", "x"])
        if e in [c.name for c in t.columns] or e in keep:
            raise Skip
        outer = rng.random() < 0.3
        ta = rng.choice(["t", "lv", "x2"])
        picked = rng.sample(fields, rng.randint(1, min(3, len(fields))))
        sel_sql = keep + [f"{e}.{f[0]}" for f in picked]
        names = keep + [f[0] for f in picked]
        if len(set(names)) != len(names):
            raise Skip
        sql = (f"{k('SELECT')} {', '.join(sel_sql)} {k('FROM')} {t.name} {k('LATERAL VIEW')} "
               f"{k('OUTER') + ' ' if outer else ''}{k('EXPLODE')}({arr}) {ta} {k('AS')} {e}")
        steps = [f'.withColumn({q(e)}, F.{"explode_outer" if outer else "explode"}({q(arr)}))']
        nf = num_fields(fields)
        if nf and rng.random() < 0.4:
            f = rng.choice(nf)
            th = threshold(f)
            sql += f" {k('WHERE')} {e}.{f[0]} >= {th}"
            steps.append(f'.filter(F.col({q(e + "." + f[0])}) >= {th})')
            feats.add("explode_filter")
        sel_py = [q(c) for c in keep] + [f'F.col({q(e + "." + f[0])})' for f in picked]
        feats |= {"lateral_view", "explode_outer" if outer else "explode", "explode_struct_fields"}
    else:
        cands = items_for(st, arr, fields, sfields, stc)
        picked = rng.sample(cands, min(len(cands), rng.randint(1, 4)))
        names = keep + [p[2] for p in picked]
        if len(set(names)) != len(names):
            raise Skip
        items_sql = keep + [p[0] + (f" {k('AS')} {p[2]}" if p[3] != "struct_field" else "") for p in picked]
        sep = ",\n    " if st.ml else ", "
        sql = f"{k('SELECT')} {sep.join(items_sql)}{chr(10) if st.ml else ' '}{k('FROM')} {t.name}"
        steps = []
        if rng.random() < 0.3:
            sql += f"{chr(10) if st.ml else ' '}{k('WHERE')} {k('SIZE')}({arr}) > 0"
            steps.append(f'.filter(F.size({q(arr)}) > 0)')
            feats.add("where_size")
        sel_py = [q(c) for c in keep] + [p[1] + (f".alias({q(p[2])})" if p[3] != "struct_field" else "") for p in picked]
        feats |= {p[3] for p in picked}
    if rng.random() < 0.35 and "id" in keep:
        o = rng.choice([n for n in names if n == "id" or n in keep])
        desc = rng.random() < 0.4
        sql += f" {k('ORDER BY')} {o}{' ' + k('DESC') if desc else ''}"
        order_cols = [o]
        feats.add("order_by")
        post = [f'.orderBy(F.col({q(o)}).desc())' if desc else f'.orderBy({q(o)})']
    else:
        post = []
    sel = (".select(\n" + "".join(f"    {s},\n" for s in sel_py) + ")") if len(", ".join(sel_py)) > 80 else f".select({', '.join(sel_py)})"
    body = "".join("    " + line + "\n" for s in [f"df_{t.name}"] + steps + [sel] + post for line in s.split("\n"))
    code = "from pyspark.sql import functions as F\n\nresult = (\n" + body + ")\n"
    return sql + (";" if st.semi else ""), code, feats, order_cols
