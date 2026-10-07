"""Targeted pairs for v14: correlated and uncorrelated scalar subqueries side by side (the v13 retry).
Only training tables are used (the hold-out tables never appear).

Why: v13 learned "scalar subquery -> separate aggregate value" from uncorrelated pairs only and then wrote correlated
scalar subqueries (`... WHERE b.k = a.k`) as a separate aggregate too (classic subquery family 6 / 17). These pairs put
both forms next to each other, so the deciding detail is whether the inner WHERE references the outer alias:

    uncorrelated   x > (SELECT AVG(b.x) FROM t b WHERE b.city = 'Paris')  -> v = df_t.filter(...).agg(F.avg("x")).first()[0]
    correlated     x > (SELECT AVG(b.x) FROM t b WHERE b.city = a.city)   -> F.avg("x").over(Window.partitionBy("city"))
    + inner filter x > (SELECT AVG(b.x) FROM t b WHERE b.city = a.city AND b.status = 'X')
                                                                          -> filtered groupBy("city") + join on "city"
    child table    c.limit > (SELECT SUM(o.amount) FROM orders o WHERE o.customer_id = c.id)
                                                                          -> groupBy("customer_id") + join; COUNT -> left
                                                                             join + F.coalesce(..., F.lit(0))

The window over the whole table always comes BEFORE an outer WHERE (the subquery sees every row of t).

Families (ids):
  scalar_uncorr    (scu)  uncorrelated: the v13 shapes (../targeted_v11/generate.py build_scalar_where) and aliased
                          same-table forms whose inner WHERE compares the would-be key with a literal
  scalar_corr_win  (scw)  same-table correlated on a non-NULL key: WHERE comparison / arithmetic, SELECT item, and
                          the inner-filter variant (filtered groupBy + join)
  scalar_corr_join (scj)  parent row vs. an aggregate of its child rows (WHERE, SELECT item, COUNT)
  scalar_mixed     (scm)  a correlated and an uncorrelated scalar subquery in the same query

Usage:  python generate.py --seed 14 --out raw_v14
"""
from __future__ import annotations

import argparse
import importlib.util
import json
from pathlib import Path

HERE = Path(__file__).resolve().parent
_spec = importlib.util.spec_from_file_location("gen_v11", HERE.parent / "targeted_v11" / "generate.py")
g = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(g)

rng, Skip, skip, wchoice = g.rng, g.Skip, g.skip, g.wchoice
TABLES, TBL, NUM, St = g.TABLES, g.TBL, g.NUM, g.St
pred, frm, finish, where_sql, filter_step, call_step = g.pred, g.frm, g.finish, g.where_sql, g.filter_step, g.call_step
order_limit, related_names, children, sql_lit, py_lit = g.order_limit, g.related_names, g.children, g.sql_lit, g.py_lit

# raw counts (~20% empty results and a few SQL already seen are dropped); build.py keeps 2,500 / 3,000 / 2,500 / 2,000
# for training + 50 validation + 50 held out per family
COUNTS = {"scalar_uncorr": 3300, "scalar_corr_win": 3900, "scalar_corr_join": 3500, "scalar_mixed": 2800}
PREFIX = {"scalar_uncorr": "scu", "scalar_corr_win": "scw", "scalar_corr_join": "scj", "scalar_mixed": "scm"}
AGG = {"AVG": "avg", "MAX": "max", "MIN": "min", "SUM": "sum", "COUNT": "count"}
HEAD = "from pyspark.sql import functions as F\n"
WIN = "from pyspark.sql.window import Window\n"


def nums(t):
    return [c for c in t.columns if c.kind in NUM and c.name != "id"]


def keys(t):
    return [c for c in t.columns if c.kind in ("cat", "fk") and not c.nullable]


def short(name):
    return name[:-3] if name.endswith("_id") else name


def alias_pair(t):
    a = t.name[0]
    return rng.choice([("a", "b"), (a, a + "2"), ("t1", "t2"), ("o", "i") if a not in "oi" else ("x", "y")])


def chain(head, steps):
    body = "".join("    " + line + "\n" for s in [head] + steps for line in s.split("\n"))
    return f"(\n{body})"


def assign(var, head, steps):
    one = var + " = " + head + "".join(steps)
    return one if len(one) <= 110 and "\n" not in one else f"{var} = {chain(head, steps)}"


def code(imports, pre, head, steps):
    return imports + "\n" + "".join(p + "\n\n" for p in pre) + "result = " + chain(head, steps) + "\n"


def outer_names(t, must, n_extra=(0, 1, 2)):
    pool = [c.name for c in t.columns if c.kind not in ("array",) and c.name not in ["id"] + must]
    extra = rng.sample(pool, min(len(pool), rng.choice(n_extra)))
    return list(dict.fromkeys(["id"] + must + extra))


def fn_for(c, choices=("AVG", "AVG", "MAX", "MIN")):
    return rng.choice([f for f in choices if f != "AVG" or c.kind in NUM])


def cmp_op(fn):
    if fn in ("MAX", "MIN") and rng.random() < 0.6:
        return "="
    # x > MAX(x) / x < MIN(x) over a set that holds x is always empty
    return rng.choice({"MAX": ["<", "<="], "MIN": [">", ">="]}.get(fn, [">", ">=", "<", "<="]))


def pyop(op):
    return {"=": "==", "<>": "!="}.get(op, op)


def corr_eq(st, a, b, key, okey=None):
    """inner correlation condition, either side first"""
    okey = okey or key
    return f"{b}.{key} = {a}.{okey}" if rng.random() < 0.75 else f"{a}.{okey} = {b}.{key}"


def inner_sql(st, item, table, alias, conds):
    k = st.k
    s = f"({k('SELECT')} {item} {k('FROM')} {frm(st, table, alias)}"
    return s + (f" {k('WHERE')} " + f" {k('AND')} ".join(conds) if conds else "") + ")"


def helper_name(fn, x, key, used):
    base = f"{AGG[fn]}_{x}_by_{short(key)}"
    v, i = base, 2
    while v in used:
        v, i = f"{base}_{i}", i + 1
    used.add(v)
    return v


def win_col(fn, x, key):
    return f'F.{fn.lower()}("{x}").over(Window.partitionBy("{key}"))'


def uncorr_value(st, t, col, fn, used, alias=None, where_p=0.6, lit_key=None):
    """uncorrelated scalar subquery -> (sql, python assignment, var); alias qualifies the inner columns"""
    k = st.k
    preds = []
    if lit_key is not None:                      # WHERE b.city = 'Paris': the key compared with a literal
        v = rng.choice(list(lit_key.pool)) if lit_key.pool else None
        if v is None:
            vals = g.column_values(t, lit_key.name)
            v = g.mid(vals)
        q = f"{alias}.{lit_key.name}" if alias else lit_key.name
        preds.append((f"{q} = {sql_lit(v)}", f'F.col("{lit_key.name}") == {py_lit(v)}'))
    elif rng.random() < where_p:
        s_, p_ = pred(t, alias, None, st)
        preds.append((s_, p_))
    base = f"{AGG[fn] if fn != 'SUM' else 'total'}_{col}"
    var, i = base, 2
    while var in used:
        var, i = f"{base}_{i}", i + 1
    used.add(var)
    item = f"{k(fn)}({alias + '.' if alias else ''}{col})"
    sql = inner_sql(st, item, t.name, alias or t.name, [s_ for s_, _ in preds])
    df = f"df_{t.name}" + "".join(f".filter({p_})" for _, p_ in preds)
    return sql, f'{var} = {df}.agg(F.{fn.lower()}("{col}")).first()[0]', var


# ---------------------------------------------------------------------------------------------------------------
# scalar_uncorr
# ---------------------------------------------------------------------------------------------------------------
def build_uncorr():
    if rng.random() < 0.5:
        sql, py, feats, oc = g.build_scalar_where()
        return sql, py, (feats - {"scalar_where"}) | {"scalar_uncorr", "uncorr_v13"}, oc
    st, used = St(), set()
    k, nl = st.k, st.nl
    t = rng.choice([x for x in TABLES if nums(x) and keys(x)])
    c = rng.choice(nums(t))
    key = rng.choice(keys(t))
    a, b = alias_pair(t)
    feats = {"scalar_uncorr", "uncorr_aliased"}
    fn = fn_for(c)
    lit = rng.random() < 0.6
    if lit:
        feats.add("uncorr_key_literal")
    s1, p1, v1 = uncorr_value(st, t, c.name, fn, used, alias=b, where_p=0.5, lit_key=key if lit else None)
    names = outer_names(t, [key.name, c.name] if rng.random() < 0.6 else [c.name])
    op = cmp_op(fn)
    conds = [(f"{a}.{c.name} {op} {s1}", f'F.col("{c.name}") {pyop(op)} {v1}')]
    if rng.random() < 0.3:
        conds.append(pred(t, a, None, st))
        if rng.random() < 0.5:
            conds = conds[::-1]
    sql = (f"{k('SELECT')} {', '.join(f'{a}.{n}' for n in names)}{nl}{k('FROM')} {frm(st, t.name, a)}{nl}"
           f"{where_sql(st, [s_ for s_, _ in conds])}")
    steps = [filter_step(p_) for _, p_ in conds] + [call_step("select", [json.dumps(n) for n in names])]
    o_sql, o_py, oc = order_limit(st, names, feats)
    if o_sql:
        sql += nl + o_sql
    return finish(st, sql), code(HEAD, [p1], f"df_{t.name}", steps + o_py), feats, oc


# ---------------------------------------------------------------------------------------------------------------
# scalar_corr_win
# ---------------------------------------------------------------------------------------------------------------
def build_corr_win():
    st, used = St(), set()
    k, nl = st.k, st.nl
    shape = wchoice([("where", 0.38), ("arith", 0.1), ("select_item", 0.27), ("inner_filter", 0.25)])
    feats = {"scalar_corr_win", "corr_" + shape}
    t = rng.choice([x for x in TABLES if keys(x) and nums(x)])
    key = rng.choice(keys(t))
    c = rng.choice(nums(t) + [x for x in t.columns if x.kind == "date"] * (shape == "select_item"))
    fn = fn_for(c) if shape != "arith" else "AVG"
    a, b = alias_pair(t)
    item = f"{k(fn)}({b}.{c.name})"
    helper = helper_name(fn, c.name, key.name, used)
    names = outer_names(t, [key.name, c.name], (0, 0, 1))
    outer = [pred(t, a, None, st, avoid=(key.name,))] if rng.random() < 0.35 else []
    if outer:
        feats.add("outer_filter")
    pre, imports = [], HEAD + WIN

    if shape == "inner_filter":
        inner_p = pred(t, b, None, st, avoid=(key.name,))
        conds = [corr_eq(st, a, b, key.name), inner_p[0]]
        if rng.random() < 0.4:
            conds = conds[::-1]
        sub = inner_sql(st, item, t.name, b, conds)
        agg_df = assign(helper.replace(f"{AGG[fn]}_", "", 1), f"df_{t.name}",
                        [f".filter({inner_p[1]})", f'.groupBy("{key.name}")', f'.agg(F.{fn.lower()}("{c.name}").alias("{helper}"))'])
        var = agg_df.split(" = ", 1)[0]
        pre, imports = [agg_df], HEAD
        op = cmp_op(fn)
        if rng.random() < 0.3:                     # SELECT item with an inner filter -> left join keeps every row
            out = rng.choice([helper, f"{AGG[fn]}_{c.name}", f"{short(key.name)}_{AGG[fn]}_{c.name}"])
            feats.add("corr_select_item")
            sql_items = [f"{a}.{n}" for n in names] + [f"{sub} {k('AS')} {out}"]
            steps = [f'.join({var}, on="{key.name}", how="left")'] + [filter_step(p_) for _, p_ in outer]
            py_items = [json.dumps(n) for n in names] + [f'F.col("{helper}")' + (f'.alias("{out}")' if out != helper else "")]
            steps.append(call_step("select", py_items))
            wsql = [s_ for s_, _ in outer]
        else:
            wsql = [f"{a}.{c.name} {op} {sub}"] + [s_ for s_, _ in outer]
            steps = [f'.join({var}, on="{key.name}", how="inner")', filter_step(f'F.col("{c.name}") {pyop(op)} F.col("{helper}")')]
            steps += [filter_step(p_) for _, p_ in outer] + [call_step("select", [json.dumps(n) for n in names])]
            sql_items = [f"{a}.{n}" for n in names]
    else:
        sub = inner_sql(st, item, t.name, b, [corr_eq(st, a, b, key.name)])
        steps = [f'.withColumn("{helper}", {win_col(fn, c.name, key.name)})']
        if shape in ("where", "arith"):
            op = cmp_op(fn) if shape == "where" else rng.choice([">", ">=", "<", "<="])
            if shape == "arith":
                f = rng.choice([0.5, 0.8, 1.2, 1.5, 2])
                wsql = [f"{a}.{c.name} {op} {f} * {sub}"]
                wpy = f'F.col("{c.name}") {op} F.col("{helper}") * {f}'
            else:
                wsql = [f"{a}.{c.name} {op} {sub}"]
                wpy = f'F.col("{c.name}") {pyop(op)} F.col("{helper}")'
            wsql += [s_ for s_, _ in outer]
            steps += [filter_step(wpy)] + [filter_step(p_) for _, p_ in outer]
            steps.append(call_step("select", [json.dumps(n) for n in names]))
            sql_items = [f"{a}.{n}" for n in names]
        else:   # select_item: the value itself or a difference / ratio
            form = wchoice([("value", 0.5), ("diff", 0.3), ("ratio", 0.2)]) if c.kind in NUM else "value"
            if form == "value":
                out = rng.choice([helper, f"{AGG[fn]}_{c.name}", f"{short(key.name)}_{AGG[fn]}_{c.name}"])
                steps[0] = f'.withColumn("{out}", {win_col(fn, c.name, key.name)})'
                sql_item, py_item = f"{sub} {k('AS')} {out}", json.dumps(out)
            elif form == "diff":
                out = rng.choice([f"diff_from_{short(key.name)}_{AGG[fn]}", f"{c.name}_minus_{AGG[fn]}", f"{c.name}_vs_{short(key.name)}"])
                sql_item = f"{a}.{c.name} - {sub} {k('AS')} {out}"
                py_item = f'(F.col("{c.name}") - F.col("{helper}")).alias("{out}")'
            else:
                out = rng.choice([f"pct_of_{short(key.name)}_{AGG[fn]}", f"{c.name}_ratio", f"{c.name}_index"])
                sql_item = f"{k('ROUND')}({a}.{c.name} * 100.0 / {sub}, 2) {k('AS')} {out}"
                py_item = f'F.round(F.col("{c.name}") * 100.0 / F.col("{helper}"), 2).alias("{out}")'
            feats.add("select_" + form)
            wsql = [s_ for s_, _ in outer]
            steps += [filter_step(p_) for _, p_ in outer]
            steps.append(call_step("select", [json.dumps(n) for n in names] + [py_item]))
            sql_items = [f"{a}.{n}" for n in names] + [sql_item]
            names = names + [out]
    sql = f"{k('SELECT')} {', '.join(sql_items)}{nl}{k('FROM')} {frm(st, t.name, a)}"
    if wsql:
        sql += nl + where_sql(st, wsql)
    o_sql, o_py, oc = order_limit(st, names, feats, allow_limit=False)
    if o_sql:
        sql += nl + o_sql
    return finish(st, sql), code(imports, pre, f"df_{t.name}", steps + o_py), feats, oc


# ---------------------------------------------------------------------------------------------------------------
# scalar_corr_join
# ---------------------------------------------------------------------------------------------------------------
def child_agg(st, parent, pa, child, fk, ca, fn, col, used, inner_p_p=0.35):
    """correlated aggregate over the child rows of pa -> (sql, py assignment, df var, value column, inner filter?)"""
    k = st.k
    conds = [corr_eq(st, pa, ca, fk.name, "id")]
    inner = pred(child, ca, None, st, avoid=(fk.name,)) if rng.random() < inner_p_p else None
    if inner:
        conds.append(inner[0])
        if rng.random() < 0.4:
            conds = conds[::-1]
    if fn == "COUNT":
        item = rng.choice([f"{k('COUNT')}(*)", f"{k('COUNT')}({ca}.id)"])
        vcol = rng.choice([f"{child.name.rstrip('s')}_count", f"n_{child.name}", f"{child.name}_count"])
        agg = f'F.count("*").alias("{vcol}")'
    else:
        item = f"{k(fn)}({ca}.{col})"
        vcol = f"{AGG[fn] if fn != 'SUM' else 'total'}_{col}"
        agg = f'F.{fn.lower()}("{col}").alias("{vcol}")'
    while vcol in used:
        vcol += "_2"
        agg = agg.rsplit(".alias(", 1)[0] + f'.alias("{vcol}")'
    used.add(vcol)
    var = f"{col if fn != 'COUNT' else child.name}_by_{short(fk.name)}"
    while var in used:
        var += "_2"
    used.add(var)
    steps = ([f".filter({inner[1]})"] if inner else []) + [f'.groupBy("{fk.name}")', f".agg({agg})"]
    return inner_sql(st, item, child.name, ca, conds), assign(var, f"df_{child.name}", steps), var, vcol, bool(inner)


def rel_pick():
    pairs = [(p, ch, fk) for p in TABLES for ch, fk in children(p) if ch.name != p.name]
    return rng.choice(pairs)


def parent_child_aliases(parent, child):
    pa, ca = parent.name[0], child.name[0]
    if pa == ca:
        ca = child.name[:2] if child.name[:2] != parent.name[:2] else ca + "2"
    return (pa, ca) if rng.random() < 0.7 else ("p", "c")


def build_corr_join():
    st, used = St(), set()
    k, nl = st.k, st.nl
    shape = wchoice([("where_col", 0.25), ("where_lit", 0.12), ("select_item", 0.38), ("where_count", 0.25)])
    feats = {"scalar_corr_join", "join_" + shape}
    parent, child, fk = rel_pick()
    pa, ca = parent_child_aliases(parent, child)
    cnums = nums(child)
    cdates = [x for x in child.columns if x.kind == "date"]
    if shape in ("where_col", "where_lit") and not cnums:
        shape = "where_count"
    if shape == "select_item":
        opts = (["COUNT"] * 2 + (["SUM", "AVG", "MAX"] if cnums else []) + (["MAX", "MIN"] if cdates else []))
        fn = rng.choice(opts)
        col = rng.choice(cdates if fn in ("MAX", "MIN") and cdates and (not cnums or rng.random() < 0.6) else cnums).name \
            if fn != "COUNT" else "id"
    elif shape == "where_count":
        fn, col = "COUNT", "id"
    else:
        fn = rng.choice(["SUM", "AVG", "MAX", "AVG"])
        col = rng.choice(cnums).name
    feats.add("child_" + fn.lower())
    sub, agg_py, var, vcol, has_inner = child_agg(st, parent, pa, child, fk, ca, fn, col, used)
    if has_inner:
        feats.add("inner_filter")
    names = outer_names(parent, [], (1, 1, 2))
    outer = [pred(parent, pa, pa, st)] if rng.random() < 0.3 else []
    if outer:
        feats.add("outer_filter")
    head = f'df_{parent.name}.alias("{pa}")'
    sel_py = [f'F.col("{pa}.{n}")' for n in names]
    sql_items = [f"{pa}.{n}" for n in names]
    v = f'F.col("{ca}.{vcol}")'
    how = "inner"
    wsql, wpy = [], []
    if shape == "select_item":
        how = "left"
        out = vcol if rng.random() < 0.5 else rng.choice([f"{child.name}_{AGG[fn]}" if fn != "COUNT" else f"num_{child.name}",
                                                            f"{AGG[fn]}_{child.name.rstrip('s')}_{col}" if fn != "COUNT" else "cnt"])
        sql_items.append(f"{sub} {k('AS')} {out}")
        if fn == "COUNT":                           # no child rows -> COUNT is 0, not NULL
            sel_py.append(f'F.coalesce({v}, F.lit(0)).alias("{out}")')
        else:
            sel_py.append(v + (f'.alias("{out}")' if out != vcol else ""))
        names = names + [out]
    elif shape == "where_count":
        how = "left"
        op, n = rng.choice([(">=", 2), (">", 1), (">=", 1), ("<", 2), ("=", 0), ("<=", 1), (">", 2), ("=", 1)])
        wsql.append(f"{sub} {op} {n}" if rng.random() < 0.8 else f"{n} {g.FLIP.get(op, op)} {sub}")
        wpy.append(f"F.coalesce({v}, F.lit(0)) {pyop(op)} {n}")
        if rng.random() < 0.3:
            sql_items.append(f"{sub} {k('AS')} {vcol}")
            sel_py.append(f'F.coalesce({v}, F.lit(0)).alias("{vcol}")')
            names = names + [vcol]
            feats.add("count_also_selected")
    else:
        op = rng.choice([">", ">=", "<", "<="])
        pn = [x for x in nums(parent) if related_names(x.name, col)] or nums(parent)
        if shape == "where_col" and pn:
            pc = rng.choice(pn).name
            wsql.append(f"{pa}.{pc} {op} {sub}" if rng.random() < 0.7 else f"{sub} {g.FLIP[op]} {pa}.{pc}")
            wpy.append(f'F.col("{pa}.{pc}") {op} {v}')
            if pc not in names:
                names.append(pc)
                sql_items.append(f"{pa}.{pc}")
                sel_py.append(f'F.col("{pa}.{pc}")')
        else:
            vals = g.column_values(child, col)
            lit = g.mid(vals)
            lit = int(round(lit * (3 if fn == "SUM" else 1))) if abs(lit) >= 10 else round(lit, 1)
            wsql.append(f"{sub} {op} {lit}")
            wpy.append(f"{v} {op} {lit}")
            feats.add("join_where_lit")
    wsql += [s_ for s_, _ in outer]
    steps = []
    if outer and rng.random() < 0.5:
        steps.append(filter_step(outer[0][1]))
        outer_py = []
    else:
        outer_py = [p_ for _, p_ in outer]
    steps.append(f'.join({var}.alias("{ca}"), F.col("{ca}.{fk.name}") == F.col("{pa}.id"), "{how}")')
    steps += [filter_step(p_) for p_ in wpy + outer_py]
    steps.append(call_step("select", sel_py))
    sql = f"{k('SELECT')} {', '.join(sql_items)}{nl}{k('FROM')} {frm(st, parent.name, pa)}"
    if wsql:
        sql += nl + where_sql(st, wsql)
    o_sql, o_py, oc = order_limit(st, names, feats)
    if o_sql:
        sql += nl + o_sql
    return finish(st, sql), code(HEAD, [agg_py], head, steps + o_py), feats, oc


# ---------------------------------------------------------------------------------------------------------------
# scalar_mixed
# ---------------------------------------------------------------------------------------------------------------
def build_mixed():
    st, used = St(), set()
    k, nl = st.k, st.nl
    shape = wchoice([("where_win", 0.4), ("select_win", 0.3), ("where_join", 0.3)])
    feats = {"scalar_mixed", "mixed_" + shape}
    if shape == "where_join":
        parent, child, fk = rel_pick()
        pn = nums(parent)
        if not pn:
            skip()
        pc = rng.choice(pn)
        pa, ca = parent_child_aliases(parent, child)
        cnums = nums(child)
        fn = rng.choice(["COUNT", "COUNT"] + (["SUM", "AVG"] if cnums else []))
        col = rng.choice(cnums).name if fn != "COUNT" else "id"
        sub, agg_py, var, vcol, _ = child_agg(st, parent, pa, child, fk, ca, fn, col, used, inner_p_p=0.25)
        ufn = rng.choice(["AVG", "AVG", "MAX", "MIN"])
        s1, p1, v1 = uncorr_value(st, parent, pc.name, ufn, used, where_p=0.4)
        uop = rng.choice([">", ">=", "<", "<="]) if ufn == "AVG" else ("<" if ufn == "MAX" else ">")
        v = f'F.col("{ca}.{vcol}")'
        if fn == "COUNT":
            op, n = rng.choice([(">=", 2), (">", 1), (">=", 1), ("=", 0), ("<", 2)])
            csql, cpy, how = f"{sub} {op} {n}", f"F.coalesce({v}, F.lit(0)) {pyop(op)} {n}", "left"
        else:
            op = rng.choice([">", "<", ">="])
            pcs = [x for x in pn if related_names(x.name, col)]
            if pcs:
                cmp_c = rng.choice(pcs).name
                csql, cpy = f"{pa}.{cmp_c} {op} {sub}", f'F.col("{pa}.{cmp_c}") {op} {v}'
            else:
                lit = g.mid(g.column_values(child, col))
                lit = int(round(lit)) if abs(lit) >= 10 else round(lit, 1)
                csql, cpy = f"{sub} {op} {lit}", f"{v} {op} {lit}"
            how = "inner"
        conds = [(f"{pa}.{pc.name} {uop} {s1}", f'F.col("{pa}.{pc.name}") {uop} {v1}'), (csql, cpy)]
        if rng.random() < 0.5:
            conds = conds[::-1]
        names = outer_names(parent, [pc.name], (0, 1, 1))
        sql = (f"{k('SELECT')} {', '.join(f'{pa}.{n}' for n in names)}{nl}{k('FROM')} {frm(st, parent.name, pa)}{nl}"
               f"{where_sql(st, [s_ for s_, _ in conds])}")
        steps = [f'.join({var}.alias("{ca}"), F.col("{ca}.{fk.name}") == F.col("{pa}.id"), "{how}")']
        steps += [filter_step(p_) for _, p_ in conds] + [call_step("select", [f'F.col("{pa}.{n}")' for n in names])]
        o_sql, o_py, oc = order_limit(st, names, feats)
        if o_sql:
            sql += nl + o_sql
        return finish(st, sql), code(HEAD, [p1, agg_py], f'df_{parent.name}.alias("{pa}")', steps + o_py), feats, oc

    t = rng.choice([x for x in TABLES if keys(x) and nums(x)])
    key = rng.choice(keys(t))
    c = rng.choice(nums(t))
    a, b = alias_pair(t)
    fn = rng.choice(["AVG", "AVG", "MAX", "MIN"])
    helper = helper_name(fn, c.name, key.name, used)
    csub = inner_sql(st, f"{k(fn)}({b}.{c.name})", t.name, b, [corr_eq(st, a, b, key.name)])
    ufn = rng.choice(["AVG", "AVG", "MAX", "MIN", "SUM"] if shape == "select_win" else ["AVG", "AVG", "MAX", "MIN"])
    ualias = rng.choice([None, "x" if "x" not in (a, b) else "z"])
    s1, p1, v1 = uncorr_value(st, t, c.name, ufn, used, alias=ualias, where_p=0.5,
                              lit_key=key if rng.random() < 0.3 else None)
    names = outer_names(t, [key.name, c.name], (0, 0, 1))
    steps = [f'.withColumn("{helper}", {win_col(fn, c.name, key.name)})']
    outer = [pred(t, a, None, st, avoid=(key.name,))] if rng.random() < 0.25 else []
    if shape == "where_win":
        op = cmp_op(fn)
        uop = rng.choice([">", ">=", "<", "<="])
        conds = [(f"{a}.{c.name} {op} {csub}", f'F.col("{c.name}") {pyop(op)} F.col("{helper}")'),
                 (f"{a}.{c.name} {uop} {s1}", f'F.col("{c.name}") {uop} {v1}')]
        if rng.random() < 0.5:
            conds = conds[::-1]
        conds += outer
        sql_items = [f"{a}.{n}" for n in names]
        steps += [filter_step(p_) for _, p_ in conds] + [call_step("select", [json.dumps(n) for n in names])]
        wsql = [s_ for s_, _ in conds]
    else:
        o1 = rng.choice([f"diff_from_{short(key.name)}_{AGG[fn]}", f"{c.name}_vs_{short(key.name)}", f"{short(key.name)}_{AGG[fn]}_{c.name}"])
        o2 = rng.choice([f"diff_from_overall_{AGG[ufn]}", f"{c.name}_vs_all", f"overall_{AGG[ufn]}_{c.name}"])
        if o1.endswith(f"_{c.name}") and not o1.startswith("diff"):
            i1 = (f"{csub} {k('AS')} {o1}", f'F.col("{helper}").alias("{o1}")')
        else:
            i1 = (f"{a}.{c.name} - {csub} {k('AS')} {o1}", f'(F.col("{c.name}") - F.col("{helper}")).alias("{o1}")')
        if ufn == "SUM":
            o2 = rng.choice(["pct_of_total", f"{c.name}_share_pct"])
            i2 = (f"{k('ROUND')}({a}.{c.name} * 100.0 / {s1}, 2) {k('AS')} {o2}", f'F.round(F.col("{c.name}") * 100.0 / {v1}, 2).alias("{o2}")')
        elif o2.startswith("overall"):
            i2 = (f"{s1} {k('AS')} {o2}", f'F.lit({v1}).alias("{o2}")')
        else:
            i2 = (f"{a}.{c.name} - {s1} {k('AS')} {o2}", f'(F.col("{c.name}") - {v1}).alias("{o2}")')
        items = [i1, i2] if rng.random() < 0.6 else [i2, i1]
        sql_items = [f"{a}.{n}" for n in names] + [s_ for s_, _ in items]
        steps += [filter_step(p_) for _, p_ in outer]
        steps.append(call_step("select", [json.dumps(n) for n in names] + [p_ for _, p_ in items]))
        names = names + [o1, o2] if items[0] is i1 else names + [o2, o1]
        wsql = [s_ for s_, _ in outer]
    sql = f"{k('SELECT')} {', '.join(sql_items)}{nl}{k('FROM')} {frm(st, t.name, a)}"
    if wsql:
        sql += nl + where_sql(st, wsql)
    o_sql, o_py, oc = order_limit(st, names, feats, allow_limit=False)
    if o_sql:
        sql += nl + o_sql
    return finish(st, sql), code(HEAD + WIN, [p1], f"df_{t.name}", steps + o_py), feats, oc


BUILDERS = {"scalar_uncorr": build_uncorr, "scalar_corr_win": build_corr_win, "scalar_corr_join": build_corr_join,
            "scalar_mixed": build_mixed}


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--seed", type=int, default=14)
    ap.add_argument("--out", default="raw_v14")
    ap.add_argument("--scale", type=float, default=1.0, help="multiply the per-family counts (debugging)")
    ap.add_argument("--families", default=None, help="comma list (default: all)")
    args = ap.parse_args()
    rng.seed(args.seed)
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    seen, metas = set(), []
    for fam in (args.families or ",".join(COUNTS)).split(","):
        n, made, tries = int(COUNTS[fam] * args.scale), 0, 0
        while made < n and tries < n * 50:
            tries += 1
            try:
                sql, py, feats, order_cols = BUILDERS[fam]()
            except Skip:
                continue
            key = " ".join(sql.lower().rstrip(";").split())
            if key in seen:
                continue
            seen.add(key)
            made += 1
            pid = f"{PREFIX[fam]}{made:05d}"
            (out / f"{pid}.sql").write_text(sql + "\n")
            (out / f"{pid}.py").write_text(py)
            metas.append({"id": pid, "family": fam, "features": sorted(feats), "order_cols": order_cols})
        print(f"{fam}: {made} pairs ({tries} tries)")
    with open(out / "meta_v14.jsonl", "w") as f:
        for m in metas:
            f.write(json.dumps(m) + "\n")
    print(f"wrote {len(metas)} pairs to {out}")


if __name__ == "__main__":
    main()
