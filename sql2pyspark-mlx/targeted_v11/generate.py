"""Targeted pairs for v11: correlated subqueries with outer columns outside the key equality, plus two smaller
families for other v10 slips. Only training tables are used (the hold-out tables never appear).

Why: on `holdout_v4_012` v10 put a correlated non-equality condition (`a.appt_date >= p.birth_date`) in the inner
`.filter()`, where the outer alias does not exist. The classic data only ever correlates on the key (`o.customer_id =
c.id`), and classic_v10/inlist.py taught "inner conditions go in the inner filter". The rule these pairs teach:

    every condition that references the outer query goes in the JOIN CONDITION, inner-only ones in the inner filter

    result = (
        df_customers.alias("c")
        .join(
            df_orders.filter(F.col("status") == "PAID").alias("o"),
            (F.col("o.customer_id") == F.col("c.id")) & (F.col("o.order_date") >= F.col("c.signup_date")),
            "left_anti",
        )
        .select(F.col("c.id"), F.col("c.name"))
    )

Families (ids):
  exists_corr  (exc)  [NOT] EXISTS with cross conditions: comparisons, DATE_ADD / DATE_SUB, DATEDIFF, BETWEEN,
                      arithmetic; parent/child, child/parent, sibling (shared foreign key) and self (manager) relations
  in_corr      (inc)  correlated x [NOT] IN (SELECT ... WHERE <outer reference>)
  exists_multi (exm)  two subquery predicates in one WHERE (two chained semi / anti joins)
  exists_agg   (exa)  a correlated subquery, then GROUP BY / HAVING (inline or in a CTE)
  setop_proj   (sop)  INTERSECT [ALL] / EXCEPT [ALL] / UNION of filtered branches: select BEFORE the set operation
  lateral_filter (lvf) LATERAL VIEW [OUTER] [POS]EXPLODE with WHERE on the exploded value: filter AFTER explode

Usage:  python generate.py --seed 11 --out raw_v11   (default counts: see COUNTS)
"""
from __future__ import annotations

import argparse
import datetime
import json
import random
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from generate_dataset import TABLES, children, fks, make_alias  # noqa: E402
from schemas import PROMO_CODES, extra_train_tables, make_rows  # noqa: E402

rng = random.Random(11)
TBL = {t.name: t for t in TABLES}
XT = {t.name: t for t in extra_train_tables()}
TIME, NUM = ("date", "ts"), ("int", "double")
FLIP = {">=": "<=", ">": "<", "<=": ">=", "<": ">"}
# raw counts: ~20% of the pairs (two-subquery pairs: ~35%) have an empty result on the sample tables and are
# dropped; build.py keeps 4,000 / 1,000 / 1,500 / 1,500 / 1,000 / 1,000 for training + 50 validation + 50 held out
# per family
COUNTS = {"exists_corr": 5600, "in_corr": 1500, "exists_multi": 3000, "exists_agg": 2000,
          "setop_proj": 1350, "lateral_filter": 1250}
# v12 (--counts v12 --or-rate 0.5): OR conditions in every family + the or_where family; build_v12.py keeps
# 2,000 or_where + 1,000 / 300 / 400 / 400 / 300 / 300 of the others for training
COUNTS_V12 = {"or_where": 3000, "exists_corr": 1350, "in_corr": 450, "exists_multi": 750, "exists_agg": 600,
              "setop_proj": 420, "lateral_filter": 380}
# v13 (--counts v13): uncorrelated scalar subqueries (WHERE / SELECT list / HAVING); build.py in targeted_v13 keeps 2,000
COUNTS_V13 = {"scalar_where": 2600}
PREFIX = {"scalar_where": "scw", "or_where": "orw", "exists_corr": "exc", "in_corr": "inc", "exists_multi": "exm", "exists_agg": "exa",
          "setop_proj": "sop", "lateral_filter": "lvf"}


class Skip(Exception):
    pass


def skip():
    raise Skip


def wchoice(pairs):
    r, acc = rng.random() * sum(w for _, w in pairs), 0.0
    for v, w in pairs:
        acc += w
        if r <= acc:
            return v
    return pairs[-1][0]


# ---------------------------------------------------------------------------------------------------------------
# sample data (the seed-7 rows the verifier uses) -> literals that keep results non-empty
# ---------------------------------------------------------------------------------------------------------------
_ROWS: dict = {}


def column_values(t, col):
    if t.name not in _ROWS:
        _ROWS[t.name] = make_rows(t, 7)
    i = [c.name for c in t.columns].index(col)
    return sorted({r[i] for r in _ROWS[t.name] if r[i] is not None and not isinstance(r[i], (list, dict, tuple))})


def mid(vals):
    if not vals:
        skip()
    n = len(vals)
    return vals[rng.randint(n // 5, max(n // 5, n - 1 - n // 5))]


def sql_lit(v, st=None, typed=False):
    if isinstance(v, bool):
        return "TRUE" if v else "FALSE"
    if isinstance(v, datetime.datetime):
        s = "'" + v.strftime("%Y-%m-%d %H:%M:%S") + "'"
        return (st.k("TIMESTAMP") + " " + s) if typed and st else s
    if isinstance(v, datetime.date):
        s = "'" + v.isoformat() + "'"
        return (st.k("DATE") + " " + s) if typed and st else s
    if isinstance(v, str):
        return "'" + v.replace("'", "''") + "'"
    return str(v)


def py_lit(v):
    if isinstance(v, datetime.datetime):
        return json.dumps(v.strftime("%Y-%m-%d %H:%M:%S"))
    if isinstance(v, datetime.date):
        return json.dumps(v.isoformat())
    if isinstance(v, bool):
        return "True" if v else "False"
    return json.dumps(v)


class St:
    """SQL surface style of one pair."""

    def __init__(self):
        self.up = rng.random() < 0.75
        self.ml = rng.random() < 0.55
        self.semi = rng.random() < 0.5
        self.use_as = rng.random() < 0.2

    def k(self, s):
        return s if self.up else s.lower()

    @property
    def nl(self):
        return "\n" if self.ml else " "


def frm(st, table, alias):
    return table if alias == table else f"{table}{' ' + st.k('AS') if st.use_as else ''} {alias}"


def finish(st, sql):
    return sql + (";" if st.semi else "")


# ---------------------------------------------------------------------------------------------------------------
# predicates
# ---------------------------------------------------------------------------------------------------------------
def pred(t, qs, qp, st, avoid=()):
    """(sql, py) filter on one column of t; qs / qp = SQL / PySpark qualifier (None = unqualified)."""
    k = st.k
    cols = [c for c in t.columns if c.name not in avoid and c.name != "id"
            and (c.kind in ("int", "double", "date", "ts", "bool") or (c.kind in ("cat", "str") and c.pool))]
    if not cols:
        skip()
    c = rng.choice(cols)
    s = f"{qs}.{c.name}" if qs else c.name
    p = f'F.col("{qp}.{c.name}")' if qp else f'F.col("{c.name}")'
    if c.nullable and rng.random() < 0.12:
        if rng.random() < 0.75:
            return f"{s} {k('IS NOT NULL')}", f"{p}.isNotNull()"
        return f"{s} {k('IS NULL')}", f"{p}.isNull()"
    if c.kind == "bool":
        if rng.random() < 0.6:
            return (s if rng.random() < 0.5 else f"{s} = {k('TRUE')}"), p
        return f"{k('NOT')} {s}", f"~{p}"
    if c.kind in ("cat", "str"):
        pool = list(c.pool)
        if len(pool) > 2 and rng.random() < 0.3:
            vals = rng.sample(pool, 2)
            neg = rng.random() < 0.2
            return (f"{s} {k('NOT IN') if neg else k('IN')} ({', '.join(sql_lit(v) for v in vals)})",
                    f"{'~' if neg else ''}{p}.isin({', '.join(py_lit(v) for v in vals)})")
        v = rng.choice(pool)
        op = rng.choice(["=", "=", "<>"])
        return f"{s} {op} {sql_lit(v)}", f"{p} {'==' if op == '=' else '!='} {py_lit(v)}"
    v = mid(column_values(t, c.name))
    if c.kind == "int":
        v = int(v)
    elif c.kind == "double":
        v = int(round(v)) if abs(v) >= 10 else round(v, 1)
    op = rng.choice([">", ">=", "<", "<="])
    return f"{s} {op} {sql_lit(v, st, typed=rng.random() < 0.3)}", f"{p} {op} {py_lit(v)}"


OR_RATE = 0.0   # v12: probability that a filter becomes an OR of two (0 keeps the v11 output byte-identical)


def or_pair(a, b, st):
    return f"({a[0]} {st.k('OR')} {b[0]})", f"({a[1]}) | ({b[1]})"


def cond(t, qs, qp, st, avoid=()):
    """pred(), or (v12, with probability OR_RATE) a parenthesized OR of two of them."""
    a = pred(t, qs, qp, st, avoid)
    if OR_RATE and rng.random() < OR_RATE:
        b = pred(t, qs, qp, st, avoid)
        if b[0] != a[0]:
            return or_pair(a, b, st)
    return a


def ccls(c):
    return "time" if c.kind in TIME else "num" if c.kind in NUM else None


def related_names(a, b):
    ta, tb = set(a.split("_")), set(b.split("_"))
    syn = {"qty": "quantity", "fee": "amount"}
    ta |= {syn.get(x, x) for x in ta}
    tb |= {syn.get(x, x) for x in tb}
    return bool(ta & tb & {"amount", "price", "cost", "quantity", "salary", "balance", "budget", "fee", "units"})


def cross_pairs(outer, inner, okey, ikey):
    out = []
    for ic in inner.columns:
        for oc in outer.columns:
            if ic.name in (ikey, "id") or oc.name in (okey, "id"):
                continue
            a, b = ccls(ic), ccls(oc)
            if a and a == b:
                w = 3 if a == "time" else (2 if related_names(ic.name, oc.name) else 1)
                out.append((ic, oc, w))
    return out


def cross(ic, oc, ia, oa, st, outer):
    """(sql, py, feature) for one condition between an inner and an outer column."""
    k = st.k
    i_s, o_s = f"{ia}.{ic.name}", f"{oa}.{oc.name}"
    i_p, o_p = f'F.col("{ia}.{ic.name}")', f'F.col("{oa}.{oc.name}")'
    flip = rng.random() < 0.3
    if ccls(ic) == "time":
        ranges = [c for c in outer.columns if c.kind in TIME and c.name != oc.name]
        form = wchoice([("cmp", 0.4), ("add", 0.18), ("diff", 0.18), ("window", 0.12), ("range", 0.12 if ranges else 0)])
        if form == "cmp":
            op = rng.choice([">=", ">", "<=", "<"])
            if flip:
                return f"{o_s} {FLIP[op]} {i_s}", f"{o_p} {FLIP[op]} {i_p}", "cross_cmp"
            return f"{i_s} {op} {o_s}", f"{i_p} {op} {o_p}", "cross_cmp"
        if form == "add":
            n = rng.choice([7, 14, 30, 60, 90, 180, 365])
            fn = rng.choice(["DATE_ADD", "DATE_ADD", "DATE_SUB"])
            op = rng.choice(["<=", ">=", "<", ">"])
            return f"{i_s} {op} {k(fn)}({o_s}, {n})", f"{i_p} {op} F.{fn.lower()}({o_p}, {n})", "cross_date_add"
        if form == "diff":
            n = rng.choice([0, 1, 7, 30, 90, 180, 365])
            op = rng.choice(["<=", ">=", "<", ">"])
            a, b, ap, bp = (o_s, i_s, o_p, i_p) if flip else (i_s, o_s, i_p, o_p)
            return f"{k('DATEDIFF')}({a}, {b}) {op} {n}", f"F.datediff({ap}, {bp}) {op} {n}", "cross_datediff"
        if form == "window":
            n = rng.choice([7, 30, 90, 365])
            return (f"{i_s} {k('BETWEEN')} {o_s} {k('AND')} {k('DATE_ADD')}({o_s}, {n})",
                    f"{i_p}.between({o_p}, F.date_add({o_p}, {n}))", "cross_between")
        hi = rng.choice(ranges)
        lo_s, hi_s = (o_s, f"{oa}.{hi.name}")
        lo_p, hi_p = (o_p, f'F.col("{oa}.{hi.name}")')
        return (f"{i_s} {k('BETWEEN')} {lo_s} {k('AND')} {hi_s}", f"{i_p}.between({lo_p}, {hi_p})", "cross_between_cols")
    form = wchoice([("cmp", 0.6), ("mult", 0.3), ("ne", 0.1)])
    if form == "cmp":
        op = rng.choice([">=", ">", "<=", "<"])
        if flip:
            return f"{o_s} {FLIP[op]} {i_s}", f"{o_p} {FLIP[op]} {i_p}", "cross_cmp"
        return f"{i_s} {op} {o_s}", f"{i_p} {op} {o_p}", "cross_cmp"
    if form == "mult":
        f = rng.choice([0.5, 0.8, 1.2, 1.5, 2])
        op = rng.choice([">=", ">", "<=", "<"])
        return f"{i_s} {op} {o_s} * {f}", f"{i_p} {op} {o_p} * {f}", "cross_arith"
    return f"{i_s} <> {o_s}", f"{i_p} != {o_p}", "cross_ne"


# ---------------------------------------------------------------------------------------------------------------
# relations between an outer and an inner table
# ---------------------------------------------------------------------------------------------------------------
def relations():
    rels = []
    for parent in TABLES:
        kids = children(parent)
        for child, fk in kids:
            rels.append({"kind": "parent_child", "outer": parent, "inner": child, "okey": "id", "ikey": fk.name,
                         "inull": fk.nullable, "onull": False})
            rels.append({"kind": "child_parent", "outer": child, "inner": parent, "okey": fk.name, "ikey": "id",
                         "inull": False, "onull": fk.nullable})
        for a, fa in kids:
            for b, fb in kids:
                if a.name != b.name:
                    rels.append({"kind": "sibling", "outer": a, "inner": b, "okey": fa.name, "ikey": fb.name,
                                 "inull": fb.nullable, "onull": fa.nullable})
    e = TBL.get("employees")    # absent from other catalogs (schema_v15)
    if e is not None:
        rels.append({"kind": "self_manager", "outer": e, "inner": e, "okey": "manager_id", "ikey": "id",
                     "inull": False, "onull": True})
        rels.append({"kind": "self_reports", "outer": e, "inner": e, "okey": "id", "ikey": "manager_id",
                     "inull": True, "onull": False})
    for r in rels:
        r["pairs"] = cross_pairs(r["outer"], r["inner"], r["okey"], r["ikey"])
    return rels


RELS = relations()
RELS_X = [r for r in RELS if r["pairs"]]


def pick_rel(pool=None):
    pool = pool or RELS_X
    w = [(r, 4 if r["kind"].startswith("self") else 2 if r["kind"] in ("parent_child", "child_parent") else 1)
         for r in pool]
    return wchoice(w)


def aliases(rel, used):
    o, i = rel["outer"].name, rel["inner"].name
    if rel["kind"] == "self_manager":
        oa, ia = rng.choice(["e", "emp"]), rng.choice(["m", "mgr", "boss"])
    elif rel["kind"] == "self_reports":
        oa, ia = rng.choice(["m", "mgr"]), rng.choice(["r", "rep", "e"])
    elif rng.random() < 0.15 and o not in used and i not in used:
        oa, ia = o, i
    else:
        oa = make_alias(o, used) if o not in used else o
        ia = make_alias(i, used)
    used |= {oa, ia}
    return oa, ia


def inner_alias(rel, used):
    if rel["kind"] == "self_manager":
        a = next(x for x in ["m", "mgr", "boss", "m2"] if x not in used)
    elif rel["kind"] == "self_reports":
        a = next(x for x in ["r", "rep", "sub", "r2"] if x not in used)
    else:
        a = make_alias(rel["inner"].name, used)
    used.add(a)
    return a


# ---------------------------------------------------------------------------------------------------------------
# rendering
# ---------------------------------------------------------------------------------------------------------------
def subquery_sql(st, item, inner_from, conds):
    k = st.k
    if st.ml:
        body = (f"    {k('SELECT')} {item}\n    {k('FROM')} {inner_from}\n    {k('WHERE')} "
                + f"\n      {k('AND')} ".join(conds))
        return f"(\n{body}\n)"
    return f"({k('SELECT')} {item} {k('FROM')} {inner_from} {k('WHERE')} " + f" {k('AND')} ".join(conds) + ")"


def where_sql(st, conds):
    k = st.k
    return f"{k('WHERE')} " + (f"\n  {k('AND')} " if st.ml else f" {k('AND')} ").join(conds)


def join_step(inner_expr, ia, conds, how):
    cond = conds[0] if len(conds) == 1 else " & ".join(f"({c})" for c in conds)
    one = f'.join({inner_expr}.alias("{ia}"), {cond}, "{how}")'
    if len(one) <= 100:
        return one
    if len(conds) == 1 or len(cond) <= 92:
        block = "    " + cond
    else:
        block = "\n".join(f"    {'& ' if j else ''}({c})" for j, c in enumerate(conds))
    return f'.join(\n    {inner_expr}.alias("{ia}"),\n{block},\n    "{how}",\n)'


def call_step(name, items, width=96):
    one = f".{name}({', '.join(items)})"
    if len(one) <= width:
        return one
    return f".{name}(\n" + "".join(f"    {i},\n" for i in items) + ")"


def code_of(head, steps, pre=""):
    body = "".join("    " + line + "\n" for s in [head] + steps for line in s.split("\n"))
    return "from pyspark.sql import functions as F\n\n" + pre + "result = (\n" + body + ")\n"


def chain_var(name, head, steps):
    body = "".join("    " + line + "\n" for s in [head] + steps for line in s.split("\n"))
    return f"{name} = (\n{body})\n\n"


# ---------------------------------------------------------------------------------------------------------------
# one correlated subquery predicate
# ---------------------------------------------------------------------------------------------------------------
def subq_pred(rel, oa, ia, st, form, anti, n_cross):
    """-> dict(sql=condition, inner=py inner DataFrame expr, conds=[py join conditions], how, outer_sql=[...],
    outer_py=[...], feats)"""
    k = st.k
    outer, inner, okey, ikey = rel["outer"], rel["inner"], rel["okey"], rel["ikey"]
    feats = {rel["kind"], "not_exists" if anti and form == "exists" else "exists" if form == "exists"
             else "not_in_corr" if anti else "in_corr"}
    outer_names = {c.name for c in outer.columns}
    crosses = []
    if n_cross:
        pool = list(rel["pairs"])
        for _ in range(n_cross):
            if not pool:
                break
            ic, oc, _w = wchoice([(p, p[2]) for p in pool])
            pool = [p for p in pool if p[0].name != ic.name]
            s, p, f = cross(ic, oc, ia, oa, st, outer)
            crosses.append((s, p))
            feats.add(f)
        if OR_RATE and crosses and rng.random() < OR_RATE * 0.4:
            s2, p2 = pred(inner, ia, ia, st, avoid=(ikey,))
            crosses[0] = (f"({crosses[0][0]} {k('OR')} {s2})", f"({crosses[0][1]}) | ({p2})")
            feats.add("cross_or_inner")
    inner_only = []
    for _ in range(rng.choice([0, 1, 1, 2])):
        unq = rng.random() < 0.2
        s, p = cond(inner, None if unq else ia, None, st, avoid=(ikey,))
        if unq and s.split()[0] in outer_names | {"NOT", "not"}:
            s, p = cond(inner, ia, None, st, avoid=(ikey,))
        inner_only.append((s, p))
    if inner_only:
        feats.add("inner_filter")
    outer_in = []
    if form == "exists" and rng.random() < 0.12:
        outer_in = [pred(outer, oa, oa, st, avoid=(okey,))]
        feats.add("outer_cond_in_subquery")

    inner_sql = [s for s, _ in inner_only]
    inner_py = [p for _, p in inner_only]
    outer_sql, outer_py = [], []
    if form == "exists":
        if rng.random() < 0.7:
            eq_s, eq_p = f"{ia}.{ikey} = {oa}.{okey}", f'F.col("{ia}.{ikey}") == F.col("{oa}.{okey}")'
        else:
            eq_s, eq_p = f"{oa}.{okey} = {ia}.{ikey}", f'F.col("{oa}.{okey}") == F.col("{ia}.{ikey}")'
        rest = [(s, p, True) for s, p in crosses + outer_in] + [(s, None, False) for s in inner_sql]
        rng.shuffle(rest)
        items = [(eq_s, eq_p, True)] + rest
        if rng.random() < 0.25 and len(items) > 1:
            j = rng.randint(1, len(items) - 1)
            items[0], items[j] = items[j], items[0]
        sub_conds = [s for s, _, _ in items]
        conds = [p for _, p, in_join in items if in_join]
        item = wchoice([("1", 0.6), ("*", 0.25), (f"{ia}.id", 0.1), (k("NULL"), 0.05)])
        cond_sql = f"{k('NOT EXISTS') if anti else k('EXISTS')} " + subquery_sql(st, item, frm(st, inner.name, ia), sub_conds)
    else:
        guard_in = anti and rel["inull"]
        guard_out = anti and rel["onull"]
        sub = [s for s, _ in crosses] + inner_sql
        rng.shuffle(sub)
        if guard_in:
            sub.append(f"{ia}.{ikey} {k('IS NOT NULL')}")
            inner_py.append(f'F.col("{ikey}").isNotNull()')
            feats.add("not_null_inner")
        conds = [f'F.col("{oa}.{okey}") == F.col("{ia}.{ikey}")'] + [p for _, p in crosses]
        cond_sql = (f"{oa}.{okey} {k('NOT IN') if anti else k('IN')} "
                    + subquery_sql(st, f"{ia}.{ikey}", frm(st, inner.name, ia), sub))
        if guard_out:
            outer_sql.append(f"{oa}.{okey} {k('IS NOT NULL')}")
            outer_py.append(f'F.col("{oa}.{okey}").isNotNull()')
            feats.add("not_null_outer")
    inner_expr = f"df_{inner.name}" + "".join(f".filter({p})" for p in inner_py)
    return {"sql": cond_sql, "inner": inner_expr, "conds": conds, "how": "left_anti" if anti else "left_semi",
            "outer_sql": outer_sql, "outer_py": outer_py, "feats": feats, "ia": ia}


def outer_select(outer, oa, okey, qualify):
    pool = [c for c in outer.columns if c.kind not in ("array", "id") and c.name != okey]
    picked = rng.sample(pool, min(len(pool), rng.randint(1, 3)))
    names = list(dict.fromkeys(["id"] + ([okey] if okey != "id" and rng.random() < 0.4 else []) + [c.name for c in picked]))
    out = []
    for n in names:
        alias = None
        if n != "id" and rng.random() < 0.12:
            alias = f"{outer.name.rstrip('s')}_{n}" if not n.startswith(outer.name.rstrip("s")) else f"the_{n}"
        out.append((n, alias))
    sql = [(f"{oa}.{n}" if qualify else n) + (f" AS {a}" if a else "") for n, a in out]
    py = [f'F.col("{oa}.{n}")' + (f'.alias("{a}")' if a else "") for n, a in out]
    return sql, py, [a or n for n, a in out]


def order_limit(st, names, feats, allow_limit=True):
    k = st.k
    if rng.random() >= 0.4:
        return "", [], []
    o = rng.choice(names)
    desc = rng.random() < 0.4
    limit = rng.choice([3, 5, 10]) if allow_limit and "id" in names and rng.random() < 0.3 else None
    tie = limit is not None and o != "id"
    sql = f"{k('ORDER BY')} {o}{' ' + k('DESC') if desc else ''}{', id' if tie else ''}"
    py = [f'.orderBy({"F.col(" + json.dumps(o) + ").desc()" if desc else json.dumps(o)}{", " + json.dumps("id") if tie else ""})']
    if limit:
        sql += f"{st.nl}{k('LIMIT')} {limit}"
        py.append(f".limit({limit})")
    feats.add("order_by")
    return sql, py, [o] + (["id"] if tie else [])


# ---------------------------------------------------------------------------------------------------------------
# families
# ---------------------------------------------------------------------------------------------------------------
def build_exists_corr(form="exists"):
    st = St()
    rel = pick_rel()
    used: set = set()
    oa, ia = aliases(rel, used)
    anti = rng.random() < 0.5
    sp = subq_pred(rel, oa, ia, st, form, anti, 1 if rng.random() < 0.8 else 2)
    outer = rel["outer"]
    qualify = oa != outer.name and rng.random() < 0.65 or oa == outer.name and rng.random() < 0.3
    star = rng.random() < 0.06 and form == "exists"
    if star:
        sel_sql, sel_py, names = [f"{oa}.*"], [f'"{oa}.*"'], [c.name for c in outer.columns]
    else:
        sel_sql, sel_py, names = outer_select(outer, oa, rel["okey"], qualify)
    feats = set(sp["feats"]) | {"exists_corr" if form == "exists" else "in_corr"}
    owhere_sql, owhere_py = [], []
    if rng.random() < 0.35:
        s, p = cond(outer, oa if qualify else None, oa, st, avoid=(rel["okey"],))
        owhere_sql.append(s)
        owhere_py.append(p)
        feats.add("outer_filter")
    conds = [sp["sql"]] + owhere_sql + sp["outer_sql"]
    if len(conds) > 1 and rng.random() < 0.5:
        conds = conds[1:] + conds[:1]
    k = st.k
    nl = st.nl
    sql = f"{k('SELECT')} {', '.join(sel_sql)}{nl}{k('FROM')} {frm(st, outer.name, oa)}{nl}{where_sql(st, conds)}"
    o_sql, o_py, order_cols = order_limit(st, names, feats) if not star else ("", [], [])
    if o_sql:
        sql += nl + o_sql
    steps = [join_step(sp["inner"], ia, sp["conds"], sp["how"])]
    steps += [f".filter({p})" for p in owhere_py + sp["outer_py"]]
    steps.append(call_step("select", sel_py))
    steps += o_py
    return finish(st, sql), code_of(f'df_{outer.name}.alias("{oa}")', steps), feats, order_cols


def build_in_corr():
    return build_exists_corr(form="in")


def build_exists_multi():
    st = St()
    outer_name = rng.choice(sorted({r["outer"].name for r in RELS}))
    pool = [r for r in RELS if r["outer"].name == outer_name]
    if len(pool) < 2:
        skip()
    r1 = pick_rel([r for r in pool if r["pairs"]] or skip())
    rest = [r for r in pool if r is not r1 and r["inner"].name != r1["inner"].name] or [r for r in pool if r is not r1]
    r2 = rng.choice(rest)
    used: set = set()
    oa, ia1 = aliases(r1, used)
    ia2 = inner_alias(r2, used)
    if r2["kind"].startswith("self") != r1["kind"].startswith("self") and outer_name == "employees":
        skip()
    combo = wchoice([("ex_nex", 0.45), ("ex_ex", 0.2), ("nex_nex", 0.15), ("ex_in", 0.2)])
    a1, a2 = {"ex_nex": (False, True), "ex_ex": (False, False), "nex_nex": (True, True), "ex_in": (False, rng.random() < 0.4)}[combo]
    p1 = subq_pred(r1, oa, ia1, st, "exists", a1, 1)
    f2 = "in" if combo == "ex_in" else "exists"
    if f2 == "in" and not r2["pairs"]:
        f2 = "exists"
    p2 = subq_pred(r2, oa, ia2, st, f2, a2, (1 if rng.random() < 0.5 else 0) if r2["pairs"] and f2 == "exists" else
                   (1 if r2["pairs"] else 0))
    if f2 == "in" and not p2["conds"][1:]:
        skip()
    outer = r1["outer"]
    qualify = rng.random() < 0.7 or oa == outer.name
    sel_sql, sel_py, names = outer_select(outer, oa, r1["okey"], qualify)
    feats = p1["feats"] | p2["feats"] | {"exists_multi", combo}
    conds = [p1["sql"], p2["sql"]] + p1["outer_sql"] + p2["outer_sql"]
    k = st.k
    nl = st.nl
    sql = f"{k('SELECT')} {', '.join(sel_sql)}{nl}{k('FROM')} {frm(st, outer.name, oa)}{nl}{where_sql(st, conds)}"
    o_sql, o_py, order_cols = order_limit(st, names, feats)
    if o_sql:
        sql += nl + o_sql
    steps = [join_step(p1["inner"], ia1, p1["conds"], p1["how"]), join_step(p2["inner"], ia2, p2["conds"], p2["how"])]
    steps += [f".filter({p})" for p in p1["outer_py"] + p2["outer_py"]]
    steps.append(call_step("select", sel_py))
    steps += o_py
    return finish(st, sql), code_of(f'df_{outer.name}.alias("{oa}")', steps), feats, order_cols


def build_exists_agg():
    st = St()
    k = st.k
    nl = st.nl
    rel = pick_rel()
    outer = rel["outer"]
    gcols = [c for c in outer.columns if c.kind == "cat" and c.pool and c.name != rel["okey"]]
    nums = [c for c in outer.columns if c.kind in NUM and c.name != "id"]
    if not gcols:
        skip()
    used: set = set()
    oa, ia = aliases(rel, used)
    anti = rng.random() < 0.45
    form = "in" if rng.random() < 0.2 else "exists"
    sp = subq_pred(rel, oa, ia, st, form, anti, 1)
    g = rng.sample(gcols, min(len(gcols), rng.choice([1, 1, 2])))
    aggs = []   # (sql_expr_fn(q), py_expr_fn(q), name)
    if rng.random() < 0.8 or not nums:
        cn = rng.choice(["n", "cnt", f"n_{outer.name}", "num_rows"])
        aggs.append((lambda q: f"{k('COUNT')}(*)", lambda q: 'F.count("*")', cn))
    for c in rng.sample(nums, min(len(nums), rng.choice([0, 1, 1, 2]))):
        fn = rng.choice(["SUM", "AVG", "MAX", "MIN", "AVG"])
        rnd = fn == "AVG" and rng.random() < 0.5
        name = {"SUM": "total", "AVG": "avg", "MAX": "max", "MIN": "min"}[fn] + "_" + c.name

        def s_(q, fn=fn, c=c, rnd=rnd):
            e = f"{k(fn)}({q}{c.name})"
            return f"{k('ROUND')}({e}, 2)" if rnd else e

        def p_(q, fn=fn, c=c, rnd=rnd):
            e = f'F.{fn.lower()}(F.col("{q}{c.name}"))'
            return f"F.round({e}, 2)" if rnd else e
        aggs.append((s_, p_, name))
    if not aggs:
        skip()
    feats = set(sp["feats"]) | {"exists_agg"}
    having = None
    if aggs[0][2] in ("n", "cnt", f"n_{outer.name}", "num_rows") and rng.random() < 0.2:
        having = rng.choice([2, 3])
        feats.add("having")
    gnames = [c.name for c in g]
    out_names = gnames + [a[2] for a in aggs]
    order_sql, order_py, order_cols = "", [], []
    if rng.random() < 0.5:
        o = rng.choice(out_names)
        desc = o not in gnames and rng.random() < 0.6
        order_sql = f"{k('ORDER BY')} {o}{' ' + k('DESC') if desc else ''}"
        order_py = [f'.orderBy({"F.col(" + json.dumps(o) + ").desc()" if desc else json.dumps(o)})']
        order_cols = [o]
        feats.add("order_by")
    cte = rng.random() < 0.4
    if cte:
        feats.add("cte")
        cname = rng.choice(["matched", "filtered", "qualified", f"{outer.name}_{'without' if anti else 'with'}_{rel['inner'].name}"])
        need = list(dict.fromkeys(["id"] + gnames + [n for n in [c.name for c in nums]
                                                      if any(n in a[2] for a in aggs)]))
        qualify = oa != outer.name
        cte_sel = [f"{oa}.{n}" if qualify else n for n in need]
        conds = [sp["sql"]] + sp["outer_sql"]
        inner_q = f"{k('SELECT')} {', '.join(cte_sel)}{nl}{k('FROM')} {frm(st, outer.name, oa)}{nl}{where_sql(st, conds)}"
        if st.ml:
            inner_q = "\n".join("    " + line for line in inner_q.split("\n"))
            head = f"{k('WITH')} {cname} {k('AS')} (\n{inner_q}\n)\n"
        else:
            head = f"{k('WITH')} {cname} {k('AS')} ({inner_q}) "
        items = gnames + [f"{s(q='')} {k('AS')} {n}" for s, _, n in aggs]
        sql = head + f"{k('SELECT')} {', '.join(items)}{nl}{k('FROM')} {cname}{nl}{k('GROUP BY')} {', '.join(gnames)}"
        if having:
            sql += f"{nl}{k('HAVING')} {k('COUNT')}(*) >= {having}"
        if order_sql:
            sql += nl + order_sql
        pre_steps = [join_step(sp["inner"], ia, sp["conds"], sp["how"])]
        pre_steps += [f".filter({p})" for p in sp["outer_py"]]
        pre_steps.append(call_step("select", [f'F.col("{oa}.{n}")' for n in need]))
        pre = chain_var(cname, f'df_{outer.name}.alias("{oa}")', pre_steps)
        steps = [call_step("groupBy", [json.dumps(n) for n in gnames]),
                 call_step("agg", [f'{p(q="")}.alias("{n}")'.replace('F.col("")', "") for _, p, n in aggs])]
        if having:
            steps.append(f'.filter(F.col("{aggs[0][2]}") >= {having})')
        steps += order_py
        return finish(st, sql), code_of(cname, steps, pre=pre), feats, order_cols
    qualify = oa != outer.name and rng.random() < 0.8
    q = f"{oa}." if qualify else ""
    items = [f"{q}{n}" for n in gnames] + [f"{s(q=q)} {k('AS')} {n}" for s, _, n in aggs]
    conds = [sp["sql"]] + sp["outer_sql"]
    sql = (f"{k('SELECT')} {', '.join(items)}{nl}{k('FROM')} {frm(st, outer.name, oa)}{nl}{where_sql(st, conds)}"
           f"{nl}{k('GROUP BY')} {', '.join(q + n for n in gnames)}")
    if having:
        sql += f"{nl}{k('HAVING')} {k('COUNT')}(*) >= {having}"
    if order_sql:
        sql += nl + order_sql
    steps = [join_step(sp["inner"], ia, sp["conds"], sp["how"])]
    steps += [f".filter({p})" for p in sp["outer_py"]]
    steps.append(call_step("groupBy", [f'F.col("{oa}.{n}")' for n in gnames]))
    steps.append(call_step("agg", [f'{p(q=oa + ".")}.alias("{n}")' for _, p, n in aggs]))
    if having:
        steps.append(f'.filter(F.col("{aggs[0][2]}") >= {having})')
    steps += order_py
    return finish(st, sql), code_of(f'df_{outer.name}.alias("{oa}")', steps), feats, order_cols


SHARED = defaultdict(list)
for _t in TABLES:
    for _c in _t.columns:
        if _c.kind in ("fk", "cat", "int", "date") and _c.name != "id":
            SHARED[(_c.name, _c.kind)].append(_t)
SHARED = {k_: v for k_, v in SHARED.items() if len(v) > 1}
SETOPS = [("INTERSECT", "intersect", 0.18), ("INTERSECT ALL", "intersectAll", 0.2), ("EXCEPT", "subtract", 0.18),
          ("EXCEPT ALL", "exceptAll", 0.16), ("EXCEPT DISTINCT", "subtract", 0.05), ("MINUS", "subtract", 0.05),
          ("UNION", "union", 0.18)]


def branch(st, t, cols, where_p=0.85):
    """SELECT cols FROM t [WHERE ...] -> (sql, [py filters], [py select items])."""
    k = st.k
    preds = []
    if rng.random() < where_p:
        preds.append(cond(t, None, None, st))
        if rng.random() < 0.2:
            preds.append(cond(t, None, None, st))
    sql = f"{k('SELECT')} {', '.join(cols)} {k('FROM')} {t.name}"
    if preds:
        sql += f" {k('WHERE')} " + f" {k('AND')} ".join(s for s, _ in preds)
    return sql, [p for _, p in preds], [json.dumps(c) for c in cols]


def build_setop():
    st = St()
    k = st.k
    mode = wchoice([("same", 0.4), ("shared", 0.35), ("parent_child", 0.25)])
    if mode == "same":
        t = rng.choice(TABLES)
        pool = [c.name for c in t.columns if c.kind in ("fk", "cat", "int", "date") and c.name != "id"]
        if not pool:
            skip()
        cols = rng.sample(pool, min(len(pool), rng.choice([1, 2, 2, 3])))
        lt, rt, lcols, rcols = t, t, cols, cols
    elif mode == "shared":
        (name, _kind), ts = rng.choice(sorted(SHARED.items(), key=lambda x: x[0]))
        lt, rt = rng.sample(ts, 2)
        common = [c.name for c in lt.columns if c.name in {x.name for x in rt.columns}
                  and c.name not in ("id", name) and c.kind in ("fk", "cat", "int", "date")
                  and c.kind == next(x for x in rt.columns if x.name == c.name).kind]
        lcols = rcols = [name] + (rng.sample(common, 1) if common and rng.random() < 0.3 else [])
    else:
        parent = rng.choice([t for t in TABLES if children(t)])
        child, fk = rng.choice(children(parent))
        if rng.random() < 0.6:
            lt, rt, lcols, rcols = parent, child, ["id"], [fk.name]
        else:
            lt, rt, lcols, rcols = child, parent, [fk.name], ["id"]
    op, method, _ = wchoice([((o, m, w), w) for o, m, w in SETOPS])
    feats = {"setop_proj", "set_" + op.lower().replace(" ", "_"), "setop_" + mode}
    ls, lf, lsel = branch(st, lt, lcols, where_p=0.85)
    rs, rf, rsel = branch(st, rt, rcols, where_p=0.85 if lt is not rt else 1.0)
    if rng.random() < 0.1:
        ls, rs = f"({ls})", f"({rs})"
        feats.add("parenthesized_branches")
    nl = st.nl
    sql = f"{ls}{nl}{k(op)}{nl}{rs}"
    order_cols, order_py = [], []
    if rng.random() < 0.35:
        o = rng.choice(lcols)
        desc = rng.random() < 0.3
        sql += f"{nl}{k('ORDER BY')} {o}{' ' + k('DESC') if desc else ''}"
        order_py = [f'.orderBy({"F.col(" + json.dumps(o) + ").desc()" if desc else json.dumps(o)})']
        order_cols = [o]
        feats.add("order_by")
    right = f"df_{rt.name}" + "".join(f".filter({p})" for p in rf) + call_step("select", rsel, width=200)
    if method == "union":
        m = "unionByName" if lcols == rcols else "union"
        set_steps = [f".{m}({right})" if len(right) < 80 else f".{m}(\n    {right}\n)", ".distinct()"]
    else:
        set_steps = [f".{method}({right})" if len(right) < 80 else f".{method}(\n    {right}\n)"]
    steps = [f".filter({p})" for p in lf] + [call_step("select", lsel)] + set_steps + order_py
    return finish(st, sql), code_of(f"df_{lt.name}", steps), feats, order_cols


def _flat_values(t, col):
    if t.name not in _ROWS:
        _ROWS[t.name] = make_rows(t, 7)
    i = [c.name for c in t.columns].index(col)
    out = []
    for r in _ROWS[t.name]:
        for v in (r[i] or []):
            if v is not None:
                out.append(v)
    return sorted(set(out))


ITEM_FIELDS = {"product_id": "int", "qty": "int", "price": "double", "promo": "cat"}
ARRAYS = [("carts", "items", "struct", 0.5), ("carts", "coupon_codes", "str", 0.15), ("products", "tags", "str", 0.15),
          ("price_tiers", "tier_limits", "int", 0.08), ("price_tiers", "tier_prices", "double", 0.06),
          ("price_tiers", "discounts", "double", 0.06)]
ELEM_NAMES = {"coupon_codes": ["code", "coupon"], "tags": ["tag", "t_name"], "tier_limits": ["lim", "tier_limit"],
              "tier_prices": ["tier_price", "unit_price"], "discounts": ["disc", "discount"]}


def build_lateral():
    st = St()
    k = st.k
    nl = st.nl
    tname, arr, etype, _ = wchoice([(a, a[3]) for a in ARRAYS])
    t = TBL.get(tname) or XT[tname]
    pos = rng.random() < 0.4
    outer = rng.random() < 0.25
    ta = rng.choice(["t", "x", "lv", "e"])
    tq = rng.choice(["c", "s", "src", "b"]) if rng.random() < 0.45 else None
    elem = rng.choice(["item", "li", "line", "it"]) if etype == "struct" else rng.choice(ELEM_NAMES[arr])
    pname = rng.choice(["pos", "pos", "idx", "i"])
    feats = {"lateral_filter", "posexplode" if pos else "explode", "outer" if outer else "inner", f"array_{etype}"}
    base_pool = [c for c in t.columns if c.kind in ("fk", "cat", "ts", "date", "int", "double") and c.name != "id"]
    keep = ["id"] + [c.name for c in rng.sample(base_pool, min(len(base_pool), rng.choice([0, 1, 1, 2])))]
    # WHERE on the exploded value (and maybe a base column / the position)
    wh_sql, wh_py = [], []

    def q(n):
        return f"{tq}.{n}" if tq else n

    if etype == "struct":
        flds = rng.sample(list(ITEM_FIELDS), rng.choice([1, 1, 2]))
        for f in flds:
            kind = ITEM_FIELDS[f]
            if kind == "cat":
                r = rng.random()
                if r < 0.35:
                    wh_sql.append(f"{elem}.{f} {k('IS NOT NULL')}")
                    wh_py.append(f'F.col("{elem}.{f}").isNotNull()')
                elif r < 0.7:
                    v = rng.choice(PROMO_CODES)
                    wh_sql.append(f"{elem}.{f} = '{v}'")
                    wh_py.append(f'F.col("{elem}.{f}") == "{v}"')
                else:
                    vs = rng.sample(PROMO_CODES, 2)
                    wh_sql.append(f"{elem}.{f} {k('IN')} ({', '.join(repr(v) for v in vs)})")
                    wh_py.append(f'F.col("{elem}.{f}").isin({", ".join(json.dumps(v) for v in vs)})')
            else:
                lo, hi = {"product_id": (2, 10), "qty": (2, 5), "price": (40, 250)}[f]
                v = rng.randint(lo, hi) if kind == "int" else rng.choice(range(lo, hi, 10))
                op = rng.choice([">", ">=", "<", "<="])
                wh_sql.append(f"{elem}.{f} {op} {v}")
                wh_py.append(f'F.col("{elem}.{f}") {op} {v}')
    else:
        vals = _flat_values(t, arr)
        if not vals:
            skip()
        if etype == "str":
            if rng.random() < 0.6:
                v = rng.choice(vals)
                wh_sql.append(f"{elem} = '{v}'")
                wh_py.append(f'F.col("{elem}") == "{v}"')
            else:
                vs = rng.sample(vals, min(2, len(vals)))
                wh_sql.append(f"{elem} {k('IN')} ({', '.join(repr(v) for v in vs)})")
                wh_py.append(f'F.col("{elem}").isin({", ".join(json.dumps(v) for v in vs)})')
        else:
            v = mid(vals)
            v = int(v) if etype == "int" else round(v, 2)
            op = rng.choice([">", ">=", "<", "<="])
            wh_sql.append(f"{elem} {op} {v}")
            wh_py.append(f'F.col("{elem}") {op} {v}')
    if pos and rng.random() < 0.35:
        op, v = rng.choice([("=", 0), ("<", 2), ("<=", 1), (">", 0)])
        wh_sql.append(f"{pname} {op} {v}")
        wh_py.append(f'F.col("{pname}") {"==" if op == "=" else op} {v}')
        feats.add("where_pos")
    if OR_RATE and len(wh_sql) >= 2 and rng.random() < OR_RATE:
        wh_sql[:2] = [f"({wh_sql[0]} {k('OR')} {wh_sql[1]})"]
        wh_py[:2] = [f"({wh_py[0]}) | ({wh_py[1]})"]
        feats.add("where_or")
    if rng.random() < 0.25:
        s, p = cond(t, tq, None, st)
        wh_sql.append(s)
        wh_py.append(p)
        feats.add("where_base")
    agg = rng.random() < 0.2 and etype in ("struct", "int", "double")
    fn = ("POSEXPLODE" if pos else "EXPLODE")
    arr_ref = q(arr)
    lv = (f"{k('LATERAL VIEW')} {k('OUTER') + ' ' if outer else ''}{k(fn)}({arr_ref}) {ta} {k('AS')} "
          + (f"{pname}, {elem}" if pos else elem))
    if pos:
        pf = "posexplode_outer" if outer else "posexplode"
        steps = [f'.select("*", F.{pf}("{arr}").alias("{pname}", "{elem}"))']
    else:
        steps = [f'.withColumn("{elem}", F.{"explode_outer" if outer else "explode"}("{arr}"))']
    steps += [f".filter({p})" for p in wh_py]
    order_cols = []
    if agg:
        gk = [c for c in keep if c != "id"][:1] or ["id"]
        if etype == "struct":
            items = [(f"{k('SUM')}({elem}.qty * {elem}.price)", f'F.sum(F.col("{elem}.qty") * F.col("{elem}.price"))',
                      rng.choice(["line_total", "cart_value", "total_value"]))]
            if rng.random() < 0.6:
                items.insert(0, (f"{k('COUNT')}(*)", 'F.count("*")', rng.choice(["n_items", "lines", "item_count"])))
        else:
            items = [(f"{k('MAX')}({elem})", f'F.max("{elem}")', f"max_{elem}"),
                     (f"{k('COUNT')}(*)", 'F.count("*")', "n")]
        sel = [q(g) for g in gk] + [f"{s} {k('AS')} {n}" for s, _, n in items]
        sql = (f"{k('SELECT')} {', '.join(sel)}{nl}{k('FROM')} {frm(st, t.name, tq) if tq else t.name}{nl}{lv}"
               + (f"{nl}{k('WHERE')} " + f" {k('AND')} ".join(wh_sql) if wh_sql else "")
               + f"{nl}{k('GROUP BY')} {', '.join(q(g) for g in gk)}")
        steps.append(call_step("groupBy", [json.dumps(g) for g in gk]))
        steps.append(call_step("agg", [f'{p}.alias("{n}")' for _, p, n in items]))
        feats.add("aggregate")
    else:
        sel_sql = [q(c) for c in keep]
        sel_py = [json.dumps(c) for c in keep]
        if pos:
            if rng.random() < 0.5:
                pn = rng.choice(["line_no", "position", "item_no"])
                sel_sql.append(f"{pname} + 1 {k('AS')} {pn}")
                sel_py.append(f'(F.col("{pname}") + 1).alias("{pn}")')
            else:
                sel_sql.append(pname)
                sel_py.append(json.dumps(pname))
        if etype == "struct":
            outs = rng.sample(list(ITEM_FIELDS), rng.choice([1, 2, 2, 3]))
            for f in outs:
                if f in keep:
                    skip()
                ren = rng.random() < 0.2
                n = {"qty": "quantity", "price": "item_price", "product_id": "item_product_id", "promo": "promo_code"}[f]
                sel_sql.append(f"{elem}.{f}" + (f" {k('AS')} {n}" if ren else ""))
                sel_py.append(f'F.col("{elem}.{f}")' + (f'.alias("{n}")' if ren else ""))
            if rng.random() < 0.25:
                sel_sql.append(f"{elem}.qty * {elem}.price {k('AS')} line_total")
                sel_py.append(f'(F.col("{elem}.qty") * F.col("{elem}.price")).alias("line_total")')
        else:
            sel_sql.append(elem)
            sel_py.append(json.dumps(elem))
        sql = (f"{k('SELECT')} {', '.join(sel_sql)}{nl}{k('FROM')} {frm(st, t.name, tq) if tq else t.name}{nl}{lv}"
               + (f"{nl}{k('WHERE')} " + f" {k('AND')} ".join(wh_sql) if wh_sql else ""))
        steps.append(call_step("select", sel_py))
        if rng.random() < 0.3:
            ob = ["id"] + ([pname] if pos and pname in [s.strip('"') for s in sel_py] else [])
            sql += f"{nl}{k('ORDER BY')} {', '.join(ob)}"
            steps.append(f".orderBy({', '.join(json.dumps(o) for o in ob)})")
            order_cols = ob
            feats.add("order_by")
    return finish(st, sql), code_of(f"df_{t.name}", steps), feats, order_cols


OR_SHAPES = [("a_or_b", 0.3), ("and_or", 0.2), ("or_and", 0.15), ("and_paren", 0.15), ("not_or", 0.1), ("or3", 0.1)]


def or_expr(st, preds, shape):
    """(sql, py) of a WHERE that mixes OR with AND / NOT; AND binds tighter than OR in SQL."""
    k = st.k
    (a, pa), (b, pb) = preds[0], preds[1]
    if shape == "a_or_b":
        return f"{a} {k('OR')} {b}", f"({pa}) | ({pb})"
    if shape == "not_or":
        return f"{k('NOT')} ({a} {k('OR')} {b})", f"~(({pa}) | ({pb}))"
    c, pc = preds[2]
    if shape == "and_or":
        sql = f"({a} {k('AND')} {b}) {k('OR')} {c}" if rng.random() < 0.6 else f"{a} {k('AND')} {b} {k('OR')} {c}"
        return sql, f"(({pa}) & ({pb})) | ({pc})"
    if shape == "or_and":
        return f"{a} {k('OR')} {b} {k('AND')} {c}", f"({pa}) | (({pb}) & ({pc}))"
    if shape == "and_paren":
        return f"{a} {k('AND')} ({b} {k('OR')} {c})", f"({pa}) & (({pb}) | ({pc}))"
    return f"{a} {k('OR')} {b} {k('OR')} {c}", f"({pa}) | ({pb}) | ({pc})"


def filter_step(py):
    one = f".filter({py})"
    return one if len(one) <= 96 else f".filter(\n    {py}\n)"


def build_or_where():
    """Single table, child JOIN parent, or GROUP BY, with a WHERE that mixes OR / AND / NOT (v12)."""
    st = St()
    k = st.k
    nl = st.nl
    mode = wchoice([("single", 0.45), ("join", 0.35), ("agg", 0.2)])
    shape = wchoice(OR_SHAPES)
    n = 2 if shape in ("a_or_b", "not_or") else 3
    feats = {"or_where", "or_" + shape, "or_" + mode}
    if mode == "join":
        child = rng.choice([t for t in TABLES if fks(t)])
        fk = rng.choice(fks(child))
        parent = TBL[fk.ref]
        used: set = set()
        ca = make_alias(child.name, used)
        pa = make_alias(parent.name, used)
        sides = [(child, ca), (parent, pa)]
        preds = []
        for j in range(n):
            t, a = sides[j % 2] if j < 2 else rng.choice(sides)
            preds.append(pred(t, a, a, st, avoid=(fk.name,)))
        if len({p[0] for p in preds}) < n:
            skip()
        wsql, wpy = or_expr(st, preds, shape)
        cpool = [c.name for c in child.columns if c.kind not in ("array", "id") and c.name != fk.name]
        ppool = [c.name for c in parent.columns if c.kind not in ("array", "id")]
        csel = ["id"] + rng.sample(cpool, min(len(cpool), rng.choice([1, 2])))
        psel = rng.sample(ppool, min(len(ppool), rng.choice([1, 2])))
        items_sql, items_py, names = [], [], []
        for c in csel:
            items_sql.append(f"{ca}.{c}")
            items_py.append(f'F.col("{ca}.{c}")')
            names.append(c)
        for c in psel:
            out = c if c not in names else f"{parent.name.rstrip('s')}_{c}"
            items_sql.append(f"{pa}.{c}" + (f" {k('AS')} {out}" if out != c else ""))
            items_py.append(f'F.col("{pa}.{c}")' + (f'.alias("{out}")' if out != c else ""))
            names.append(out)
        left = rng.random() < 0.2
        jk = k("LEFT JOIN") if left else k(rng.choice(["JOIN", "JOIN", "INNER JOIN"]))
        sql = (f"{k('SELECT')} {', '.join(items_sql)}{nl}{k('FROM')} {frm(st, child.name, ca)}{nl}"
               f"{jk} {frm(st, parent.name, pa)} {k('ON')} {ca}.{fk.name} = {pa}.id{nl}{k('WHERE')} {wsql}")
        o_sql, o_py, order_cols = order_limit(st, names, feats)
        if o_sql:
            sql += nl + o_sql
        steps = [f'.join(df_{parent.name}.alias("{pa}"), F.col("{ca}.{fk.name}") == F.col("{pa}.id"), '
                 f'"{"left" if left else "inner"}")', filter_step(wpy), call_step("select", items_py)] + o_py
        return finish(st, sql), code_of(f'df_{child.name}.alias("{ca}")', steps), feats, order_cols
    t = rng.choice(TABLES)
    tq = None
    if mode == "single" and rng.random() < 0.3:
        tq = make_alias(t.name, set())
    preds = [pred(t, tq, None, st) for _ in range(n)]
    if len({p[0] for p in preds}) < n:
        skip()
    wsql, wpy = or_expr(st, preds, shape)
    q = (lambda c: f"{tq}.{c}") if tq else (lambda c: c)
    if mode == "agg":
        gcols = [c for c in t.columns if c.kind == "cat" and c.pool]
        nums = [c for c in t.columns if c.kind in NUM and c.name != "id"]
        if not gcols:
            skip()
        g = rng.choice(gcols).name
        aggs = [(f"{k('COUNT')}(*)", 'F.count("*")', rng.choice(["n", "cnt", "num_rows"]))]
        if nums and rng.random() < 0.7:
            c = rng.choice(nums).name
            fn = rng.choice(["SUM", "AVG", "MAX"])
            aggs.append((f"{k(fn)}({c})", f'F.{fn.lower()}("{c}")', f"{fn.lower()}_{c}"))
        sql = (f"{k('SELECT')} {g}, " + ", ".join(f"{s_} {k('AS')} {n_}" for s_, _, n_ in aggs)
               + f"{nl}{k('FROM')} {t.name}{nl}{k('WHERE')} {wsql}{nl}{k('GROUP BY')} {g}")
        steps = [filter_step(wpy), call_step("groupBy", [json.dumps(g)]),
                 call_step("agg", [f'{p_}.alias("{n_}")' for _, p_, n_ in aggs])]
        order_cols = []
        if rng.random() < 0.5:
            sql += f"{nl}{k('ORDER BY')} {g}"
            steps.append(f'.orderBy("{g}")')
            order_cols = [g]
            feats.add("order_by")
        return finish(st, sql), code_of(f"df_{t.name}", steps), feats, order_cols
    pool = [c.name for c in t.columns if c.kind not in ("array", "id")]
    names = ["id"] + rng.sample(pool, min(len(pool), rng.randint(1, 3)))
    sql = (f"{k('SELECT')} {', '.join(q(c) for c in names)}{nl}{k('FROM')} {frm(st, t.name, tq) if tq else t.name}"
           f"{nl}{k('WHERE')} {wsql}")
    o_sql, o_py, order_cols = order_limit(st, names, feats)
    if o_sql:
        sql += nl + o_sql
    steps = [filter_step(wpy), call_step("select", [json.dumps(c) for c in names])] + o_py
    return finish(st, sql), code_of(f"df_{t.name}", steps), feats, order_cols


AGG_NAMES = {"MAX": "max", "MIN": "min", "AVG": "avg", "SUM": "total"}


def scalar_sub(st, t, col, fn, var_used, where_p=0.7):
    """Uncorrelated scalar subquery on t.col -> (sql, py_assignment, var). The inner WHERE is filtered first."""
    k = st.k
    preds = [cond(t, None, None, st)] if rng.random() < where_p else []
    if preds and rng.random() < 0.15:
        preds.append(pred(t, None, None, st))
    base = f"{AGG_NAMES[fn]}_{col}"
    var, i = base, 2
    while var in var_used:
        var, i = f"{base}_{i}", i + 1
    var_used.add(var)
    sql = f"({k('SELECT')} {k(fn)}({col}) {k('FROM')} {t.name}" + (
        f" {k('WHERE')} " + f" {k('AND')} ".join(s_ for s_, _ in preds) if preds else "") + ")"
    df = f"df_{t.name}" + "".join(f".filter({p_})" for _, p_ in preds)
    py = f'{var} = {df}.agg(F.{fn.lower()}("{col}")).first()[0]'
    return sql, py, var


def build_scalar_where():
    """WHERE / SELECT / HAVING with an uncorrelated scalar subquery -> .agg(...).first()[0] + a Python value (v13)."""
    st = St()
    k = st.k
    nl = st.nl
    shape = wchoice([("cmp", 0.35), ("arith", 0.15), ("between", 0.1), ("select_item", 0.15), ("having", 0.12),
                     ("join", 0.13)])
    feats = {"scalar_where", "scalar_" + shape}
    nums_of = lambda t: [c for c in t.columns if c.kind in NUM and c.name != "id"]  # noqa: E731
    t = rng.choice([x for x in TABLES if nums_of(x)])
    c = rng.choice(nums_of(t))
    other = t
    if rng.random() < 0.3:
        cands = [(x, y) for x in TABLES if x is not t for y in nums_of(x) if related_names(y.name, c.name)]
        if cands:
            other, oc = rng.choice(cands)
            feats.add("scalar_other_table")
        else:
            oc = c
    else:
        oc = c
    used: set = set()
    pre, steps = [], []
    fn = rng.choice(["AVG", "AVG", "MAX", "MIN", "SUM"]) if shape not in ("between",) else None
    pool = [x.name for x in t.columns if x.kind not in ("array",) and x.name not in ("id", c.name)]
    names = ["id"] + rng.sample(pool, min(len(pool), rng.choice([0, 1, 2]))) + [c.name]
    names = list(dict.fromkeys(names))
    order_cols = []
    if shape in ("cmp", "arith", "between"):
        extra = [cond(t, None, None, st)] if rng.random() < 0.3 else []
        if shape == "between":
            s1, p1, v1 = scalar_sub(st, other, oc.name, "MIN", used)
            s2, p2, v2 = scalar_sub(st, other, oc.name, "AVG", used)
            wsql = f"{c.name} {k('BETWEEN')} {s1} {k('AND')} {s2}"
            wpy = f'F.col("{c.name}").between({v1}, {v2})'
            pre += [p1, p2]
        else:
            fn = rng.choice(["AVG", "AVG", "MAX", "MIN"]) if shape == "cmp" else "AVG"
            s1, p1, v1 = scalar_sub(st, other, oc.name, fn, used)
            pre.append(p1)
            op = "=" if fn in ("MAX", "MIN") and rng.random() < 0.3 else rng.choice([">", ">=", "<", "<="])
            if shape == "arith":
                f = rng.choice([0.5, 0.8, 1.2, 1.5, 2])
                wsql = f"{c.name} {op} {f} * {s1}"
                # F.lit keeps an empty subquery (None) a NULL, as in SQL; a plain Python `f * None` would raise
                wpy = f'F.col("{c.name}") {op} F.lit({v1}) * {f}'
                feats.add("scalar_arith")
            else:
                wsql = f"{c.name} {op} {s1}"
                wpy = f'F.col("{c.name}") {"==" if op == "=" else op} {v1}'
        conds_sql, conds_py = [wsql] + [s_ for s_, _ in extra], [wpy] + [p_ for _, p_ in extra]
        if len(conds_sql) > 1 and rng.random() < 0.5:
            conds_sql, conds_py = conds_sql[::-1], conds_py[::-1]
        sql = f"{k('SELECT')} {', '.join(names)}{nl}{k('FROM')} {t.name}{nl}{where_sql(st, conds_sql)}"
        steps += [filter_step(p_) for p_ in conds_py]
        o_sql, o_py, order_cols = order_limit(st, names, feats)
        if o_sql:
            sql += nl + o_sql
        steps += [call_step("select", [json.dumps(n) for n in names])] + o_py
    elif shape == "select_item":
        fn = rng.choice(["AVG", "AVG", "MAX", "SUM"])
        s1, p1, v1 = scalar_sub(st, other, oc.name, fn, used, where_p=0.5)
        pre.append(p1)
        if fn == "SUM":
            out = rng.choice([f"pct_of_total_{c.name}", "pct_of_total", "share_pct"])
            item_sql = f"{k('ROUND')}({c.name} * 100.0 / {s1}, 2) {k('AS')} {out}"
            item_py = f'F.round(F.col("{c.name}") * 100.0 / {v1}, 2).alias("{out}")'
        else:
            out = rng.choice([f"{c.name}_minus_{AGG_NAMES[fn]}", f"diff_from_{AGG_NAMES[fn]}", f"{c.name}_vs_{AGG_NAMES[fn]}"])
            item_sql = f"{c.name} - {s1} {k('AS')} {out}"
            item_py = f'(F.col("{c.name}") - {v1}).alias("{out}")'
        sql = f"{k('SELECT')} {', '.join(names)}, {item_sql}{nl}{k('FROM')} {t.name}"
        extra = [cond(t, None, None, st)] if rng.random() < 0.4 else []
        if extra:
            sql += f"{nl}{where_sql(st, [s_ for s_, _ in extra])}"
            steps += [filter_step(p_) for _, p_ in extra]
        o_sql, o_py, order_cols = order_limit(st, names, feats)
        if o_sql:
            sql += nl + o_sql
        steps += [call_step("select", [json.dumps(n) for n in names] + [item_py])] + o_py
    elif shape == "having":
        gcols = [x for x in t.columns if x.kind == "cat" and x.pool]
        if not gcols:
            skip()
        g = rng.choice(gcols).name
        fn = rng.choice(["AVG", "AVG", "MAX", "SUM"])
        an = f"{AGG_NAMES[fn]}_{c.name}"
        s1, p1, v1 = scalar_sub(st, other, oc.name, "AVG", used, where_p=0.5)
        pre.append(p1)
        op = rng.choice([">", ">=", "<"])
        sql = (f"{k('SELECT')} {g}, {k(fn)}({c.name}) {k('AS')} {an}{nl}{k('FROM')} {t.name}{nl}{k('GROUP BY')} {g}"
               f"{nl}{k('HAVING')} {k(fn)}({c.name}) {op} {s1}")
        steps += [call_step("groupBy", [json.dumps(g)]), f'.agg(F.{fn.lower()}("{c.name}").alias("{an}"))',
                  f'.filter(F.col("{an}") {op} {v1})']
        if rng.random() < 0.5:
            sql += f"{nl}{k('ORDER BY')} {an} {k('DESC')}"
            steps.append(f'.orderBy(F.col("{an}").desc())')
            order_cols = [an]
            feats.add("order_by")
    else:   # join: child JOIN parent, scalar filter on a child column
        child = rng.choice([x for x in TABLES if fks(x) and nums_of(x)])
        fk = rng.choice(fks(child))
        parent = TBL[fk.ref]
        cc = rng.choice(nums_of(child))
        u2: set = set()
        ca, pa = make_alias(child.name, u2), make_alias(parent.name, u2)
        fn = rng.choice(["AVG", "AVG", "MAX", "MIN"])
        s1, p1, v1 = scalar_sub(st, child, cc.name, fn, used)
        pre.append(p1)
        op = rng.choice([">", ">=", "<", "<="])
        ppool = [x.name for x in parent.columns if x.kind not in ("array", "id")]
        pcol = rng.choice(ppool)
        out = pcol if pcol not in [x.name for x in child.columns] else f"{parent.name.rstrip('s')}_{pcol}"
        items_sql = [f"{ca}.id", f"{ca}.{cc.name}", f"{pa}.{pcol}" + (f" {k('AS')} {out}" if out != pcol else "")]
        items_py = [f'F.col("{ca}.id")', f'F.col("{ca}.{cc.name}")', f'F.col("{pa}.{pcol}")' + (f'.alias("{out}")' if out != pcol else "")]
        sql = (f"{k('SELECT')} {', '.join(items_sql)}{nl}{k('FROM')} {frm(st, child.name, ca)}{nl}{k('JOIN')} "
               f"{frm(st, parent.name, pa)} {k('ON')} {ca}.{fk.name} = {pa}.id{nl}{k('WHERE')} {ca}.{cc.name} {op} {s1}")
        o_sql, o_py, order_cols = order_limit(st, ["id", cc.name, out], feats)
        if o_sql:
            sql += nl + o_sql
        steps = [f'.join(df_{parent.name}.alias("{pa}"), F.col("{ca}.{fk.name}") == F.col("{pa}.id"), "inner")',
                 filter_step(f'F.col("{ca}.{cc.name}") {op} {v1}'), call_step("select", items_py)] + o_py
        code = "from pyspark.sql import functions as F\n\n" + "\n".join(pre) + "\n\n" + code_of(
            f'df_{child.name}.alias("{ca}")', steps).split("\n\n", 1)[1]
        return finish(st, sql), code, feats, order_cols
    code = "from pyspark.sql import functions as F\n\n" + "\n".join(pre) + "\n\n" + code_of(
        f"df_{t.name}", steps).split("\n\n", 1)[1]
    return finish(st, sql), code, feats, order_cols


BUILDERS = {"scalar_where": build_scalar_where, "or_where": build_or_where, "exists_corr": build_exists_corr, "in_corr": build_in_corr, "exists_multi": build_exists_multi,
            "exists_agg": build_exists_agg, "setop_proj": build_setop, "lateral_filter": build_lateral}


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--seed", type=int, default=11)
    ap.add_argument("--out", default="raw_v11")
    ap.add_argument("--scale", type=float, default=1.0, help="multiply the per-family counts (debugging)")
    ap.add_argument("--families", default=None, help="comma list (default: every family of --counts)")
    ap.add_argument("--counts", choices=["v11", "v12", "v13"], default="v11")
    ap.add_argument("--or-rate", type=float, default=0.0, help="v12: chance that a filter becomes an OR of two")
    args = ap.parse_args()
    global OR_RATE
    OR_RATE = args.or_rate
    counts = {"v11": COUNTS, "v12": COUNTS_V12, "v13": COUNTS_V13}[args.counts]
    rng.seed(args.seed)
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    seen, metas = set(), []
    for fam in (args.families or ",".join(counts)).split(","):
        n, made, tries = int(counts[fam] * args.scale), 0, 0
        while made < n and tries < n * 50:
            tries += 1
            try:
                sql, code, feats, order_cols = BUILDERS[fam]()
            except Skip:
                continue
            key = " ".join(sql.lower().rstrip(";").split())
            if key in seen:
                continue
            seen.add(key)
            made += 1
            pid = f"{PREFIX[fam]}{made:05d}"
            (out / f"{pid}.sql").write_text(sql + "\n")
            (out / f"{pid}.py").write_text(code)
            metas.append({"id": pid, "family": fam, "features": sorted(feats), "order_cols": order_cols})
        print(f"{fam}: {made} pairs ({tries} tries)")
    with open(out / f"meta_{args.counts}.jsonl", "w") as f:
        for m in metas:
            f.write(json.dumps(m) + "\n")
    print(f"wrote {len(metas)} pairs to {out}")


if __name__ == "__main__":
    main()
