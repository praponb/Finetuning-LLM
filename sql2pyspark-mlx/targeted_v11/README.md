# targeted_v11 — correlated subqueries with outer columns outside the key equality (+ set operations, LATERAL VIEW filters)

Training data for **v11**, the follow-up to v10 ([classic_v10](../classic_v10/README.md)).

## Why

* **`holdout_v4_012` (new in v10).** For a correlated `NOT EXISTS` whose subquery compares an inner column with an outer
  one (`a.appt_date >= p.birth_date`), v10 put that comparison in the inner `.filter()`, where the outer alias `p` does
  not exist, instead of in the join condition. The classic data only ever correlates on the key (`o.customer_id =
  c.id`), and classic_v10's `IN` / `NOT IN` pairs taught "subquery conditions go in the inner filter". These pairs
  teach the full rule: **every condition that references the outer query goes in the join condition; only inner-only
  conditions go in the inner `.filter()`**.
* Two smaller v10 slips on the v4 hold-out: a projection placed *after* `INTERSECT ALL` (`holdout_v4_017`) and a
  `WHERE` on an exploded value applied *before* the explode (`holdout_v4_021`).

```python
# SELECT c.id, c.name FROM customers c
# WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id AND o.status = 'PAID'
#                   AND o.order_date >= c.signup_date)
result = (
    df_customers.alias("c")
    .join(
        df_orders.filter(F.col("status") == "PAID").alias("o"),
        (F.col("o.customer_id") == F.col("c.id"))
        & (F.col("o.order_date") >= F.col("c.signup_date")),
        "left_anti",
    )
    .select(F.col("c.id"), F.col("c.name"))
)
```

## What is in it

| Family (id prefix) | Train | What |
| --- | --- | --- |
| `exists_corr` (`exc`) | 4,000 | `[NOT] EXISTS` with 1–2 cross conditions: comparisons (either side first), `DATE_ADD` / `DATE_SUB`, `DATEDIFF`, `BETWEEN o.x AND DATE_ADD(o.x, n)`, `BETWEEN o.start AND o.end`, arithmetic (`i.x > o.y * 1.5`), `<>`; sometimes an outer-only condition inside the subquery (it also belongs in the join condition: for `NOT EXISTS` it is not an outer filter). Relations: parent → child, child → parent, siblings sharing a foreign key (`subscriptions` / `orders` on `customer_id`) and self (`employees` manager / reports). Aliases short, with `AS`, or the table names themselves. |
| `in_corr` (`inc`) | 1,000 | correlated `x [NOT] IN (SELECT i.k FROM ... WHERE <outer reference>)`; `NOT IN` over nullable keys gets explicit `IS NOT NULL` guards |
| `exists_multi` (`exm`) | 1,500 | two subquery predicates in one `WHERE` (`EXISTS` + `NOT EXISTS`, two of a kind, `EXISTS` + `IN`) → two chained semi / anti joins |
| `exists_agg` (`exa`) | 1,500 | a correlated subquery, then `GROUP BY` (+ `HAVING`, `ORDER BY`), inline or with the filtered rows in a CTE |
| `setop_proj` (`sop`) | 1,000 | `INTERSECT [ALL]` / `EXCEPT [ALL \| DISTINCT]` / `MINUS` / `UNION` of filtered branches (same table, tables sharing a column, parent `id` vs. child key): each branch is filtered and **selected before** the set operation |
| `lateral_filter` (`lvf`) | 1,000 | `LATERAL VIEW [OUTER] [POS]EXPLODE` over `carts.items` (structs), `carts.coupon_codes`, `products.tags`, `price_tiers` arrays with a `WHERE` on the exploded value / position: explode first, **then filter**; sometimes followed by `GROUP BY` |

**Spark verification: 14,700 / 14,700 raw pairs match** (`raw_v11/verify_report.json`); 2,661 have an empty result on
the sample tables and are dropped, none of the rest is in an eval set. Only training tables are used (the hold-out
tables of `eval/holdout*.jsonl` never appear). Literals come from the
seed-7 sample rows the verifier uses, so most results are non-empty.

| Path | Content |
| --- | --- |
| `generate.py` | the six generators (`--seed 11 --out raw_v11`) |
| `build.py` | drops empty results and any SQL found in an eval set, then per family: 50 held out (`data/heldout_meta.jsonl`), 50 validation, the rest of the quota for training; writes `data/` and `data_mix/` |
| `data/` | `train.jsonl` (10,000), `valid.jsonl` (300), `heldout_meta.jsonl` (300, evaluated with `eval/evaluate.py --set`), `split_stats.json` |
| `data_mix/` | what v11 trains on (15,000, mean ~1,430 characters per sample): the 10,000 new pairs + 5,000 pairs sampled from `classic_v10/data_mix` (mirrored classic, `IN` / `NOT IN`, plain classic, complex_v9); validation 300 new + 300 v10 |
| `lora_config.yaml` | continues from v10 (`../classic_v10/adapters`), 4,000 micro-batches, writes `targeted_v11/adapters/` |

The held-out pairs come from the same generators as the training pairs (new SQL, same templates), so they measure
whether v11 learned these forms, not generalization; `holdout_v4_012`, `017` and `021` remain the unseen checks.

## Rebuild

```bash
cd sql2pyspark-mlx/targeted_v11
../.venv/bin/python generate.py --seed 11 --out raw_v11                       # 14,700 raw pairs
../.venv/bin/python ../verify_pairs.py --src raw_v11 --workers 5 2>/dev/null   # ~11 min
../.venv/bin/python build.py
```

Train, export and evaluate: `zsh eval/start_training_v11.sh` from the repo root.

## Results (v11, 2026-10-02)

Trained 2026-10-02 05:55 → 11:59 (6 h 04 min, peak 9.7 GB), validation loss 0.072 → 0.009 → 0.008 → 0.007 → **0.007**
(`train_v11.log`).

| Eval set | v9-q4 (`latest`) | v10-q4 | v11-q4 |
| --- | --- | --- | --- |
| Held-out targeted pairs (300, `data/heldout_meta.jsonl`) | 130 | 83 | **283** |
| … correlated `EXISTS` / `IN` / two subqueries / + `GROUP BY` / set ops / `LATERAL VIEW` (50 each) | 18 / 4 / 19 / 27 / 45 / 17 | 9 / 5 / 5 / 10 / 41 / 13 | **47 / 46 / 44 / 48 / 49 / 49** |
| v4 hold-out (25) | 21 | 18 | **21** (`012`, `017`, `021` fixed) |
| Hold-out (62) | 59 | 60 | 60 |
| Fixed test (150) / classic (97) | 140 / 97 | **147 / 97** | 143 / 93 |
| complex_v8 test (100) | **87** | 85 | 84 |

The rule was learned. One side effect: these pairs always write one `.filter()` per condition and never contain `OR`,
and v11 split a `WHERE a OR b` into two `.filter()` calls (AND) on 2 fixed-test queries. Next version: add `OR` /
mixed `AND`-`OR` conditions to the generators. Details: [README → v11 results](../../README.md#v11-results-2026-10-02).
