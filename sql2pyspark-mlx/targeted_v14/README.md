# targeted_v14 — correlated and uncorrelated scalar subqueries side by side (the v13 retry)

Training data for **v14**, a retry of v13 ([targeted_v13](../targeted_v13/README.md)) that continues from **v12**
([targeted_v12](../targeted_v12/README.md)), not from v13.

## Why

v13 trained on 2,000 *uncorrelated* scalar subqueries (`.agg(...).first()[0]`) with a small classic replay. It fixed
`gen01245` / `gen05869` but then wrote *correlated* scalar subqueries (`... WHERE b.k = a.k`) as a separate aggregate too
(classic `subquery` family 6 / 17, hold-out 52 / 62). The data never showed the neighbouring correlated form, and the
validation set did not contain it, so the validation loss did not warn.

v14 puts both forms next to each other. What decides the translation is whether the inner `WHERE` refers to the outer
alias:

| SQL | PySpark |
| --- | --- |
| `x > (SELECT AVG(b.x) FROM t b WHERE b.city = 'Paris')` (uncorrelated) | `v = df_t.filter(...).agg(F.avg("x")).first()[0]`, `.filter(F.col("x") > v)` |
| `x > (SELECT AVG(b.x) FROM t b WHERE b.city = a.city)` (correlated, same table) | `.withColumn("avg_x_by_city", F.avg("x").over(Window.partitionBy("city")))` before any outer filter |
| `... WHERE b.city = a.city AND b.status = 'X'` (correlated + inner filter) | `df_t.filter(...).groupBy("city").agg(...)`, `.join(..., on="city", how="inner")` |
| `c.limit > (SELECT SUM(o.amount) FROM orders o WHERE o.customer_id = c.id)` (child rows) | `df_orders.groupBy("customer_id").agg(...)`, join on `customer_id = c.id` |
| `(SELECT COUNT(*) FROM orders o WHERE o.customer_id = c.id)` | left join + `F.coalesce(F.col("o.n_orders"), F.lit(0))` (no child rows -> 0, not NULL) |

## What is in it

[`generate.py`](generate.py) (reuses the helpers and the v13 `build_scalar_where` of
[`../targeted_v11/generate.py`](../targeted_v11/generate.py)), 10,000 train pairs in 4 families:

| Family | Train | Content |
| --- | --- | --- |
| `scalar_uncorr` (`scu`) | 2,500 | half the v13 shapes (comparison, arithmetic, `BETWEEN`, `SELECT` item, `HAVING`, join), half aliased same-table forms (`FROM t a ... (SELECT MAX(b.x) FROM t b WHERE b.k = 'literal')`) |
| `scalar_corr_win` (`scw`) | 3,000 | same-table correlated on a non-NULL key: `WHERE` comparison / arithmetic, `SELECT` item (value, difference, ratio), each with an optional outer filter applied *after* the window; plus the inner-filter variant (filtered `groupBy` + join) |
| `scalar_corr_join` (`scj`) | 2,500 | a parent row against an aggregate of its child rows: `WHERE` against a parent column or a literal, `SELECT` item (`SUM` / `AVG` / `MAX` / `MIN` / `COUNT`, also dates), `COUNT` comparisons |
| `scalar_mixed` (`scm`) | 2,000 | a correlated and an uncorrelated scalar subquery in one query (`WHERE`, `SELECT` list, parent + child) |

Each family also has 50 validation and 50 held-out pairs (`data/heldout_meta.jsonl`, 200 in total). Empty results and
any SQL that is already in an eval set, in the classic test split or in the targeted_v11 / v12 / v13 data are dropped.

| Path | Content |
| --- | --- |
| `generate.py` | the 4 families above |
| `build.py` | the split above; `data/` and `data_mix/` |
| `data_mix/` | what v14 trains on (~25,000): 10,000 new + all 708 classic scalar-subquery pairs + 5,000 other classic + 500 classic `OR` + 4,500 complex_v9 + 2,500 targeted_v12 + 1,800 targeted_v11; validation = 200 new + the 36 classic scalar-subquery validation pairs + 150 v12 |
| `lora_config.yaml` | continues from v12 (`../targeted_v12/adapters`), 4,800 micro-batches (2x v13), writes `targeted_v14/adapters/` |

## Rebuild

```bash
cd sql2pyspark-mlx/targeted_v14
../.venv/bin/python generate.py --seed 14 --out raw_v14                      # 13,500 raw pairs
../.venv/bin/python ../verify_pairs.py --src raw_v14 --workers 5 2>/dev/null  # ~25 min
../.venv/bin/python build.py
```

Train, export and evaluate: `zsh eval/start_training_v14.sh` from the repo root.

## Results (v14, 2026-10-07) — not promoted

Trained 2026-10-06 14:21 → 10-07 00:47 (peak 10.0 GB; ~2.5 h of it with the Mac asleep), validation loss 0.055 → 0.015 →
0.010 → 0.008 → 0.006 → 0.006 → 0.006 (`train_v14.log`). v14-q4 vs. v12-q4:

* **The target was hit without v13's over-generalization**: classic `subquery` family **17 / 15** of 17 (v13-q4 6),
  **196 / 96** of the 200 held-out pairs (per family 50 / 50 / 50 / 46), 150 / 123 v13 held-out pairs, fixed test 145 / 143.
* **But names drift on unseen schemas**: `apppt_date`, `num_stores` for `num_stops`, `name` for `stop_name`, `ss.restock`
  for `ss.revenue` — hold-out 58 / 59, v4 hold-out 18 / 21, v11 / v12 held-out 280 / 227 (287 / 230), complex 82 / 83.
* The iter-2,400 checkpoint (`sql2pyspark:v14-c2400-q4`, `eval/eval_v14_ckpt.sh`) has the same slips (hold-out 58, v4 19,
  fixed test 139, held-out 190 / 200), so the cause is the data — 10,000 more pairs on the same 17 tables — not the
  training length.

v12-q4 stays `latest`. A next attempt would need more table / column-name variety, not more pairs on these tables.
Details: [README → v14 results](../../README.md#v14-results-2026-10-07--not-promoted).
