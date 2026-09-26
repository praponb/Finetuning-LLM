"""Expression / query objects that render to BOTH Spark SQL and PySpark.

Every object has ``sql(style)`` and ``py(ctx)``. Because both renderings come
from the same object tree, a generated pair agrees by construction; the
execution check in verify_pairs.py confirms it.

PySpark output conventions (applied everywhere):
- ``from pyspark.sql import functions as F`` (+ ``Window`` when needed)
- input DataFrames are ``df_<table>``; the final DataFrame is ``result``
- the final column list / order / aliases match the SQL select list
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field, fields, is_dataclass, replace
from typing import Any, cast

from schemas import Column, Table

KIND = {"id": "int", "fk": "int", "int": "int", "double": "double", "cat": "str", "str": "str",
        "date": "date", "ts": "ts", "bool": "bool", "array": "array"}


def pyq(v) -> str:
    return json.dumps(v, ensure_ascii=False)


def fmt_num(v) -> str:
    if isinstance(v, bool):
        return str(v)
    if isinstance(v, int):
        return str(v)
    r = round(v, 2)
    if r == int(r) and abs(r) >= 10:
        return str(int(r))
    return repr(r)


class Style:
    """Surface variation of the SQL text (never affects semantics)."""

    def __init__(self, rng):
        self.upper = rng.random() < 0.78
        self.multiline = rng.random() < 0.45
        self.semicolon = rng.random() < 0.55
        self.use_as = rng.random() < 0.85
        self.neq = rng.choice(["<>", "!="])
        self.inner_kw = rng.choice(["JOIN", "INNER JOIN", "INNER JOIN"])
        self.outer_kw = rng.random() < 0.3
        self.short_alias = rng.random() < 0.85
        self.explicit_asc = rng.random() < 0.15

    def k(self, s: str) -> str:
        return s if self.upper else s.lower()


class Ctx:
    """Collects PySpark preamble statements (window specs, scalar lookups)."""

    def __init__(self):
        self.windows: dict[str, str] = {}
        self.pre: list[str] = []
        self.uses_window = False

    def window_var(self, spec_py: str) -> str:
        self.uses_window = True
        if spec_py not in self.windows:
            name = "w" if not self.windows else f"w{len(self.windows) + 1}"
            self.windows[spec_py] = name
            self.pre.append(f"{name} = {spec_py}")
        return self.windows[spec_py]


# ---------------------------------------------------------------------------
# Expressions
# ---------------------------------------------------------------------------
class E:
    """Base expression: every subclass renders to Spark SQL (``sql``) and PySpark (``py``)."""
    kind: str = "double"
    is_pred = False

    def sql(self, st: Style) -> str:
        raise NotImplementedError

    def py(self, ctx: Ctx) -> str:
        raise NotImplementedError

    def arg(self, ctx):
        """Rendering where a PySpark function accepts a column *name* string."""
        return self.py(ctx)

    def rhs(self, ctx):
        """Rendering on the right side of a comparison / as a when() value."""
        return self.py(ctx)

    def pyw(self, ctx):
        """Rendering safe for method chaining (``.alias``, ``.cast`` ...)."""
        return self.py(ctx)


@dataclass(eq=False)
class Col(E):
    c: Column
    q: str | None = None
    kind: str = field(init=False, default="")

    def __post_init__(self):
        self.kind = KIND[self.c.kind]

    @property
    def name(self):
        return self.c.name

    def ref(self):
        return f"{self.q}.{self.c.name}" if self.q else self.c.name

    def sql(self, st):
        return self.ref()

    def py(self, ctx):
        return f"F.col({pyq(self.ref())})"

    def arg(self, ctx):
        return pyq(self.ref()) if self.q is None else self.py(ctx)


@dataclass(eq=False)
class Lit(E):
    v: object
    kind: str = field(init=False, default="")

    def __post_init__(self):
        if isinstance(self.v, bool):
            self.kind = "bool"
        elif isinstance(self.v, int):
            self.kind = "int"
        elif isinstance(self.v, float):
            self.kind = "double"
        else:
            self.kind = "str"

    def sql(self, st):
        if self.v is None:
            return st.k("NULL")
        if isinstance(self.v, bool):
            return st.k("TRUE" if self.v else "FALSE")
        if isinstance(self.v, str):
            return "'" + self.v + "'"
        return fmt_num(self.v)

    def raw(self):
        if isinstance(self.v, (int, float)) and not isinstance(self.v, bool):
            return fmt_num(self.v)
        return pyq(self.v) if isinstance(self.v, str) else repr(self.v)

    def py(self, ctx):
        return f"F.lit({self.raw()})"

    def rhs(self, ctx):
        return self.raw()


@dataclass(eq=False)
class Raw(E):
    """A non-column function parameter (format string, length, separator ...)."""
    v: object

    def sql(self, st):
        return "'" + self.v + "'" if isinstance(self.v, str) else fmt_num(self.v)

    def py(self, ctx):
        return pyq(self.v) if isinstance(self.v, str) else fmt_num(self.v)


@dataclass(eq=False)
class BinOp(E):
    l: E
    op: str
    r: E
    kind: str = "double"

    def sql(self, st):
        def w(x):
            return f"({x.sql(st)})" if isinstance(x, BinOp) else x.sql(st)
        return f"{w(self.l)} {self.op} {w(self.r)}"

    def py(self, ctx):
        return f"({self.l.rhs(ctx)} {self.op} {self.r.rhs(ctx)})"


@dataclass(eq=False)
class Fn(E):
    sql_name: str
    py_name: str
    args: list
    kind: str = "double"
    suffix_sql: str = ""
    suffix_py: str = ""

    def sql(self, st):
        return f"{st.k(self.sql_name)}({', '.join(a.sql(st) for a in self.args)}){self.suffix_sql}"

    def py(self, ctx):
        return f"F.{self.py_name}({', '.join(a.arg(ctx) for a in self.args)}){self.suffix_py}"


@dataclass(eq=False)
class NoArgFn(E):
    sql_name: str
    py_name: str
    kind: str = "date"

    def sql(self, st):
        return f"{st.k(self.sql_name)}()"

    def py(self, ctx):
        return f"F.{self.py_name}()"


@dataclass(eq=False)
class Cast(E):
    e: E
    to: str  # SQL type name, e.g. INT
    kind: str = field(init=False, default="")

    def __post_init__(self):
        self.kind = {"INT": "int", "BIGINT": "int", "DOUBLE": "double", "STRING": "str", "DATE": "date"}.get(
            self.to, "double")

    def sql(self, st):
        return f"{st.k('CAST')}({self.e.sql(st)} {st.k('AS')} {st.k(self.to)})"

    def py(self, ctx):
        return f"{self.e.pyw(ctx)}.cast({pyq(self.to.lower())})"


@dataclass(eq=False)
class Case(E):
    branches: list          # [(pred, value)]
    else_: E | None = None
    kind: str = "str"
    if_form: bool = False   # render as IF(cond, a, b)

    def sql(self, st):
        k = st.k
        if self.if_form:
            (p, v), = self.branches
            assert self.else_ is not None, "IF() needs an else value"
            return f"{k('IF')}({p.sql(st)}, {v.sql(st)}, {self.else_.sql(st)})"
        s = k("CASE")
        for p, v in self.branches:
            s += f" {k('WHEN')} {p.sql(st)} {k('THEN')} {v.sql(st)}"
        if self.else_ is not None:
            s += f" {k('ELSE')} {self.else_.sql(st)}"
        return s + " " + k("END")

    def py(self, ctx):
        s = ""
        for i, (p, v) in enumerate(self.branches):
            s += ("F.when" if i == 0 else ".when") + f"({p.py(ctx)}, {v.rhs(ctx)})"
        if self.else_ is not None:
            s += f".otherwise({self.else_.rhs(ctx)})"
        return s


AGG_PY = {"COUNT": "count", "SUM": "sum", "AVG": "avg", "MIN": "min", "MAX": "max",
          "STDDEV": "stddev", "COLLECT_SET": "collect_set"}


@dataclass(eq=False)
class Agg(E):
    func: str
    e: E | None = None
    distinct: bool = False
    kind: str = "double"

    def sql(self, st):
        k = st.k
        if self.e is None:
            return f"{k(self.func)}(*)"
        d = k("DISTINCT") + " " if self.distinct else ""
        return f"{k(self.func)}({d}{self.e.sql(st)})"

    def py(self, ctx):
        if self.e is None:
            return f'F.{AGG_PY[self.func]}("*")'
        if self.distinct:
            return f"F.countDistinct({self.e.arg(ctx)})"
        return f"F.{AGG_PY[self.func]}({self.e.arg(ctx)})"


@dataclass(eq=False)
class WinSpec:
    part: list = field(default_factory=list)    # [E]
    order: list = field(default_factory=list)   # [(E, desc)]
    frame: tuple | None = None                  # (lo, hi): "UP", "CR", "UF" or -n

    def sql(self, st):
        k = st.k
        parts = []
        if self.part:
            parts.append(f"{k('PARTITION BY')} {', '.join(e.sql(st) for e in self.part)}")
        if self.order:
            parts.append(f"{k('ORDER BY')} " + ", ".join(
                e.sql(st) + (" " + k("DESC") if d else "") for e, d in self.order))
        if self.frame:
            def b(x):
                return {"UP": "UNBOUNDED PRECEDING", "CR": "CURRENT ROW", "UF": "UNBOUNDED FOLLOWING"}.get(
                    x, f"{-x} PRECEDING" if isinstance(x, int) else x)
            parts.append(k(f"ROWS BETWEEN {b(self.frame[0])} AND {b(self.frame[1])}"))
        return " ".join(parts)

    def py(self, ctx):
        s = "Window" if (self.part or self.order) else "Window.partitionBy()"
        if self.part:
            s += f".partitionBy({', '.join(e.arg(ctx) for e in self.part)})"
        if self.order:
            s += ".orderBy(" + ", ".join(
                f"{e.pyw(ctx)}.desc()" if d else e.arg(ctx) for e, d in self.order) + ")"
        if self.frame:
            def b(x):
                return {"UP": "Window.unboundedPreceding", "CR": "Window.currentRow",
                        "UF": "Window.unboundedFollowing"}.get(x, str(x))
            s += f".rowsBetween({b(self.frame[0])}, {b(self.frame[1])})"
        return s


@dataclass(eq=False)
class WinFn(E):
    sql_name: str
    py_name: str
    args: list
    spec: WinSpec
    kind: str = "int"

    def sql(self, st):
        return (f"{st.k(self.sql_name)}({', '.join(a.sql(st) for a in self.args)}) "
                f"{st.k('OVER')} ({self.spec.sql(st)})")

    def py(self, ctx):
        w = ctx.window_var(self.spec.py(ctx))
        return f"F.{self.py_name}({', '.join(a.arg(ctx) for a in self.args)}).over({w})"


@dataclass(eq=False)
class PyVar(E):
    """A scalar computed up-front in PySpark (e.g. an uncorrelated scalar subquery)."""
    sql_text: str
    var: str
    kind: str = "double"

    def sql(self, st):
        return self.sql_text

    def py(self, ctx):
        return self.var

    rhs = py


# ---------------------------------------------------------------------------
# Predicates
# ---------------------------------------------------------------------------
PY_OP = {"=": "==", "<>": "!=", "!=": "!=", "<": "<", "<=": "<=", ">": ">", ">=": ">="}


class P(E):
    kind = "bool"
    is_pred = True
    paren_py = False   # needs parentheses when combined with & | ~

    def pyw(self, ctx):
        return f"({self.py(ctx)})" if self.paren_py else self.py(ctx)


@dataclass(eq=False)
class Cmp(P):
    l: E
    op: str
    r: E
    paren_py = True

    def sql(self, st):
        op = st.neq if self.op in ("<>", "!=") else self.op
        return f"{self.l.sql(st)} {op} {self.r.sql(st)}"

    def py(self, ctx):
        return f"{self.l.pyw(ctx)} {PY_OP[self.op]} {self.r.rhs(ctx)}"


@dataclass(eq=False)
class InList(P):
    e: E
    vals: list
    neg: bool = False

    def sql(self, st):
        return (f"{self.e.sql(st)} {st.k('NOT IN') if self.neg else st.k('IN')} "
                f"({', '.join(Lit(v).sql(st) for v in self.vals)})")

    def py(self, ctx):
        return f"{'~' if self.neg else ''}{self.e.pyw(ctx)}.isin([{', '.join(Lit(v).raw() for v in self.vals)}])"


@dataclass(eq=False)
class Between(P):
    e: E
    lo: object
    hi: object

    def sql(self, st):
        return f"{self.e.sql(st)} {st.k('BETWEEN')} {Lit(self.lo).sql(st)} {st.k('AND')} {Lit(self.hi).sql(st)}"

    def py(self, ctx):
        return f"{self.e.pyw(ctx)}.between({Lit(self.lo).raw()}, {Lit(self.hi).raw()})"


@dataclass(eq=False)
class Like(P):
    e: E
    pat: str
    neg: bool = False

    def sql(self, st):
        return f"{self.e.sql(st)} {st.k('NOT LIKE') if self.neg else st.k('LIKE')} '{self.pat}'"

    def py(self, ctx):
        return f"{'~' if self.neg else ''}{self.e.pyw(ctx)}.like({pyq(self.pat)})"


@dataclass(eq=False)
class IsNull(P):
    e: E
    neg: bool = False

    def sql(self, st):
        return f"{self.e.sql(st)} {st.k('IS NOT NULL') if self.neg else st.k('IS NULL')}"

    def py(self, ctx):
        return f"{self.e.pyw(ctx)}.{'isNotNull' if self.neg else 'isNull'}()"


@dataclass(eq=False)
class BoolTest(P):
    e: E
    neg: bool = False
    explicit: bool = False   # "x = TRUE" instead of "x"

    def sql(self, st):
        if self.explicit:
            return f"{self.e.sql(st)} = {st.k('FALSE' if self.neg else 'TRUE')}"
        return f"{st.k('NOT')} {self.e.sql(st)}" if self.neg else self.e.sql(st)

    def py(self, ctx):
        return f"~{self.e.pyw(ctx)}" if self.neg else self.e.py(ctx)


@dataclass(eq=False)
class FnPred(P):
    """Boolean-valued function, e.g. ARRAY_CONTAINS."""
    fn: Fn

    def sql(self, st):
        return self.fn.sql(st)

    def py(self, ctx):
        return self.fn.py(ctx)


@dataclass(eq=False)
class And(P):
    parts: list
    paren_py = True

    def sql(self, st):
        return f" {st.k('AND')} ".join(f"({p.sql(st)})" if isinstance(p, Or) else p.sql(st) for p in self.parts)

    def py(self, ctx):
        return " & ".join(p.pyw(ctx) for p in self.parts)


@dataclass(eq=False)
class Or(P):
    parts: list
    paren_py = True

    def sql(self, st):
        return f" {st.k('OR')} ".join(f"({p.sql(st)})" if isinstance(p, And) else p.sql(st) for p in self.parts)

    def py(self, ctx):
        return " | ".join(p.pyw(ctx) for p in self.parts)


@dataclass(eq=False)
class Not(P):
    p: P

    def sql(self, st):
        return f"{st.k('NOT')} ({self.p.sql(st)})"

    def py(self, ctx):
        return f"~({self.p.py(ctx)})"


def find(e, cls):
    """All sub-expressions of type ``cls`` (depth first)."""
    if isinstance(e, cls):
        yield e
    if isinstance(e, (list, tuple)):
        for x in e:
            yield from find(x, cls)
    elif is_dataclass(e) and isinstance(e, (E, WinSpec)):
        for f in fields(cast(Any, e)):
            yield from find(getattr(e, f.name), cls)


def requal(e, q):
    """Deep copy of an expression tree with every Col re-qualified to ``q``."""
    if isinstance(e, Col):
        return replace(e, q=q)
    if isinstance(e, (list, tuple)):
        return type(e)(requal(x, q) for x in e)
    if is_dataclass(e) and not isinstance(e, type) and isinstance(e, (E, WinSpec)):
        return replace(cast(Any, e), **{f.name: requal(getattr(e, f.name), q) for f in fields(cast(Any, e))
                             if isinstance(getattr(e, f.name), (E, WinSpec, list, tuple))})
    return e


# ---------------------------------------------------------------------------
# Relations and SELECT
# ---------------------------------------------------------------------------
@dataclass(eq=False)
class Rel:
    sql_name: str
    py_var: str
    cols: list
    alias: str | None = None
    table: Table | None = None
    sql_alias: str | None = None    # FROM-clause alias of a derived table (not used to qualify columns)
    sub: "Sel | None" = None        # derived relation (CTE / subquery)
    sub_sql_inline: bool = False    # render sub-select inside FROM (derived table)
    py_inline: bool = False         # chain the sub-select's PySpark steps instead of a variable

    def col(self, name):
        return next(c for c in self.cols if c.name == name)

    def ref(self, col):
        return Col(col, self.alias)

    def py_head(self):
        return self.py_var + (f".alias({pyq(self.alias)})" if self.alias else "")


@dataclass(eq=False)
class Join:
    rel: Rel
    how: str          # inner / left / right / full / cross
    cond: E | None = None


@dataclass(eq=False)
class Semi:
    rel: Rel          # inner relation (alias used in EXISTS form and in PySpark)
    anti: bool
    form: str         # "exists" | "in"
    outer: Col
    inner_col: Column
    where: E | None = None    # inner filter, unqualified
    guard: bool = False       # NOT IN over a nullable column -> isNotNull() first


def call(name: str, args: list[str], width: int = 84) -> str:
    one = f".{name}({', '.join(args)})"
    if len(one) <= width and "\n" not in one:
        return one
    return f".{name}(\n" + "".join(f"    {a},\n" for a in args) + ")"


def chain(head: str, steps: list[str]) -> str:
    if not steps:
        return head
    one = head + "".join(steps)
    if len(steps) == 1 and len(one) <= 84 and "\n" not in one:
        return one
    body = "\n".join("    " + line for s in [head] + steps for line in s.split("\n"))
    return "(\n" + body + "\n)"


def sel_sql(e, n, st):
    if isinstance(e, Col) and e.name == n:
        return e.sql(st)
    return f"{e.sql(st)} {st.k('AS') + ' ' if st.use_as else ''}{n}"


def sel_py(e, n, ctx):
    if isinstance(e, Col) and e.name == n:
        return e.arg(ctx)
    return f"{e.pyw(ctx)}.alias({pyq(n)})"


@dataclass(eq=False)
class Sel:
    base: Rel
    items: list = field(default_factory=list)     # [(E, name)]
    joins: list = field(default_factory=list)
    semis: list = field(default_factory=list)
    where: E | None = None
    star: bool = False                            # SELECT *, <items as extra columns>
    group: list | None = None                     # [(E, name)]
    group_mode: str = "plain"                     # plain / rollup / cube
    group_ref: str = "expr"                       # expr / alias / ordinal
    aggs: list = field(default_factory=list)      # [(Agg-ish E, name)]
    hidden: list = field(default_factory=list)    # aggregates only used by HAVING
    having: list = field(default_factory=list)    # [(E, name, op, value, use_alias)]
    post: list = field(default_factory=list)      # [(sql_item, E_py_over_output, name)] e.g. rank over agg
    distinct: bool = False
    order: list = field(default_factory=list)     # [(name, desc, sql_text|None)]
    limit: int | None = None

    @property
    def is_agg(self):
        return self.group is not None or bool(self.aggs)

    def out_cols(self) -> list[tuple[str, E]]:
        if self.is_agg:
            base = list((n, e) for e, n in (self.group or [])) + [(n, e) for e, n in self.aggs]
            return base + [(n, e) for _, e, n in self.post]
        if self.star:
            return [(c.name, Col(c)) for c in self.base.cols] + [(n, e) for e, n in self.items]
        return [(n, e) for e, n in self.items]

    def out_names(self):
        return [n for n, _ in self.out_cols()]

    # ---------------- SQL ----------------
    def from_sql(self, st, pad):
        b = self.base
        if b.sub is not None and b.sub_sql_inline:
            inner = b.sub.sql(st, pad + "    ")
            if st.multiline:
                src = f"(\n{pad}    {inner}\n{pad})"
            else:
                src = f"({inner})"
            return f"{src} {st.k('AS') + ' ' if st.use_as else ''}{b.sql_alias or b.alias or 'sub'}"
        return self.rel_sql(b, st)

    @staticmethod
    def rel_sql(r, st):
        if r.alias and r.alias != r.sql_name:
            return f"{r.sql_name} {st.k('AS') + ' ' if not st.short_alias else ''}{r.alias}"
        return r.sql_name

    def join_sql(self, j, st, pad):
        k = st.k
        if j.how == "cross":
            kw = "CROSS JOIN"
        elif j.how == "inner":
            kw = st.inner_kw
        else:
            kw = {"left": "LEFT", "right": "RIGHT", "full": "FULL"}[j.how] + (" OUTER" if st.outer_kw or j.how == "full" else "") + " JOIN"
        if j.rel.sub is not None and j.rel.sub_sql_inline:
            src = f"({j.rel.sub.sql(st, pad + '    ')}) {j.rel.alias}"
        else:
            src = self.rel_sql(j.rel, st)
        s = f"{k(kw)} {src}"
        if j.cond is not None:
            s += f" {k('ON')} {j.cond.sql(st)}"
        return s

    def semi_sql(self, s, st):
        k = st.k
        if s.form == "exists":
            inner_where = [f"{Col(s.inner_col, s.rel.alias).sql(st)} = {s.outer.sql(st)}"]
            if s.where is not None:
                w = requal(s.where, s.rel.alias)
                inner_where.append(f"({w.sql(st)})" if isinstance(w, Or) else w.sql(st))
            body = (f"{k('SELECT')} 1 {k('FROM')} {self.rel_sql(s.rel, st)} {k('WHERE')} "
                    + f" {k('AND')} ".join(inner_where))
            return f"{k('NOT EXISTS') if s.anti else k('EXISTS')} ({body})"
        body = f"{k('SELECT')} {s.inner_col.name} {k('FROM')} {s.rel.sql_name}"
        if s.where is not None:
            body += f" {k('WHERE')} {s.where.sql(st)}"
        return f"{s.outer.sql(st)} {k('NOT IN') if s.anti else k('IN')} ({body})"

    def sql(self, st, pad=""):
        k = st.k
        nl = ("\n" + pad) if st.multiline else " "
        head = k("SELECT") + (" " + k("DISTINCT") if self.distinct else "")
        if self.is_agg:
            items = [sel_sql(e, n, st) for e, n in (self.group or []) + self.aggs]
            items += [s for s, _, _ in self.post]
        elif self.star:
            items = ["*"] + [sel_sql(e, n, st) for e, n in self.items]
        else:
            items = [sel_sql(e, n, st) for e, n in self.items]
        if st.multiline and len(items) > 3:
            select = head + f"\n{pad}    " + f",\n{pad}    ".join(items)
        else:
            select = head + " " + ", ".join(items)
        parts = [select, k("FROM") + " " + self.from_sql(st, pad)]
        parts += [self.join_sql(j, st, pad) for j in self.joins]
        conds = []
        if self.where is not None:
            w = self.where.sql(st)
            conds.append(f"({w})" if isinstance(self.where, Or) and self.semis else w)
        conds += [self.semi_sql(s, st) for s in self.semis]
        if conds:
            parts.append(k("WHERE") + " " + f" {k('AND')} ".join(conds))
        if self.group:
            if self.group_ref == "ordinal":
                keys = [str(i + 1) for i in range(len(self.group))]
            elif self.group_ref == "alias":
                keys = [n for _, n in self.group]
            else:
                keys = [e.sql(st) for e, _ in self.group]
            g = ", ".join(keys)
            if self.group_mode != "plain":
                g = f"{k(self.group_mode.upper())}({g})"
            parts.append(f"{k('GROUP BY')} {g}")
        if self.having:
            hs = []
            for e, n, op, v, use_alias in self.having:
                lhs = n if use_alias else e.sql(st)
                hs.append(f"{lhs} {st.neq if op in ('<>', '!=') else op} {Lit(v).sql(st)}")
            parts.append(f"{k('HAVING')} " + f" {k('AND')} ".join(hs))
        if self.order:
            os_ = []
            for n, d, sql_text in self.order:
                t = sql_text or n
                os_.append(t + (" " + k("DESC") if d else (" " + k("ASC") if st.explicit_asc else "")))
            parts.append(f"{k('ORDER BY')} " + ", ".join(os_))
        if self.limit:
            parts.append(f"{k('LIMIT')} {self.limit}")
        return nl.join(parts)

    # ---------------- PySpark ----------------
    def py_parts(self, ctx) -> tuple[str, list[str]]:
        b = self.base
        if b.sub is not None and b.py_inline:
            head, steps = b.sub.py_parts(ctx)
            if b.alias and (self.joins or self.semis):
                steps.append(f".alias({pyq(b.alias)})")
        else:
            head, steps = b.py_head(), []
        for j in self.joins:
            if j.how == "cross":
                steps.append(f".crossJoin({j.rel.py_head()})")
            else:
                how = {"full": "outer"}.get(j.how, j.how)
                steps.append(f".join({j.rel.py_head()}, {j.cond.py(ctx)}, {pyq(how)})")
        for s in self.semis:
            var = ""
            inner = s.rel.py_var
            if s.where is not None:
                inner += f".filter({s.where.py(ctx)})"
            if s.guard:
                # SQL `x NOT IN (subquery)` drops rows where x is NULL, unless the subquery returns no rows
                var = f"excluded_{s.rel.sql_name}"
                ctx.pre.append(f"{var} = {inner}.select({pyq(s.inner_col.name)})")
                inner = var
            inner += f".alias({pyq(s.rel.alias)})"
            cond = f"{s.outer.pyw(ctx)} == {Col(s.inner_col, s.rel.alias).py(ctx)}"
            steps.append(f".join({inner}, {cond}, {pyq('left_anti' if s.anti else 'left_semi')})")
            if s.guard:
                steps.append(f".filter({s.outer.pyw(ctx)}.isNotNull() | F.lit({var}.isEmpty()))")
        if self.where is not None:
            steps.append(f".filter({self.where.py(ctx)})")
        if self.is_agg:
            if self.group:
                keys = [sel_py(e, n, ctx) for e, n in self.group]
                method = {"plain": "groupBy", "rollup": "rollup", "cube": "cube"}[self.group_mode]
                steps.append(call(method, keys))
            steps.append(call("agg", [f"{e.pyw(ctx)}.alias({pyq(n)})" for e, n in self.aggs + self.hidden],
                              width=60 if len(self.aggs + self.hidden) > 1 else 84))
            if self.having:
                hs = [Cmp(Col(_named(n, e)), op, Lit(v)) for e, n, op, v, _ in self.having]
                p = hs[0] if len(hs) == 1 else And(hs)
                steps.append(f".filter({p.py(ctx)})")
            if self.hidden:
                steps.append(f".drop({', '.join(pyq(n) for _, n in self.hidden)})")
            for _, e, n in self.post:
                steps.append(f".withColumn({pyq(n)}, {e.py(ctx)})")
        elif self.star:
            for e, n in self.items:
                steps.append(f".withColumn({pyq(n)}, {e.py(ctx)})")
        else:
            steps.append(call("select", [sel_py(e, n, ctx) for e, n in self.items]))
        if self.distinct:
            steps.append(".distinct()")
        if self.order:
            os_ = [f"F.col({pyq(n)}).desc()" if d else pyq(n) for n, d, _ in self.order]
            steps.append(call("orderBy", os_))
        if self.limit:
            steps.append(f".limit({self.limit})")
        return head, steps

    def py_chain(self, ctx):
        return chain(*self.py_parts(ctx))


def _named(n, e):
    return Column(n, {"int": "int", "double": "double", "str": "str", "date": "date"}.get(e.kind, "double"))


def out_columns(sel: Sel, src_cols_by_name=None) -> list[Column]:
    """Column descriptors for a derived relation built from ``sel``."""
    cols = []
    for n, e in sel.out_cols():
        if isinstance(e, Col):
            cols.append(replace(e.c, name=n))
        elif isinstance(e, Agg) and e.func == "COUNT":
            cols.append(Column(n, "int", lo=1, hi=12, nullable=False))
        elif isinstance(e, Agg) and e.func == "SUM" and isinstance(e.e, Col):
            cols.append(Column(n, "double", lo=e.e.c.lo * 2, hi=e.e.c.hi * 6))
        elif isinstance(e, Agg) and isinstance(e.e, Col) and e.e.c.kind in ("date", "ts"):
            cols.append(replace(e.e.c, name=n))
        elif isinstance(e, Agg) and isinstance(e.e, Col):
            cols.append(Column(n, "double", lo=e.e.c.lo, hi=e.e.c.hi))
        else:
            kind = {"int": "int", "double": "double", "str": "str", "date": "date", "ts": "ts",
                    "bool": "bool", "array": "array"}.get(e.kind, "double")
            cols.append(Column(n, kind, lo=0, hi=100))
    return cols


def render_code(ctx: Ctx, defs: list[tuple[str, str]], result: str) -> str:
    lines = ["from pyspark.sql import functions as F"]
    if ctx.uses_window:
        lines.append("from pyspark.sql.window import Window")
    lines.append("")
    if ctx.pre:
        lines += ctx.pre + [""]
    for var, ch in defs:
        lines += [f"{var} = {ch}", ""]
    lines.append(f"result = {result}")
    return "\n".join(lines)
