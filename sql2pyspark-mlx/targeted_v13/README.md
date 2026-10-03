# targeted_v13 — uncorrelated scalar subqueries (+ a larger complex replay), the follow-up to targeted_v12

Training data for **v13**, the follow-up to v12 ([targeted_v12](../targeted_v12/README.md)).

## Why

* v12's two remaining classic misses (`gen01245`, `gen05869`) are `x >= (SELECT MAX(x) FROM t WHERE ...)`: v12 built the
  aggregate as a separate DataFrame and compared against its column (`max_df["max_x"]`), which Spark rejects
  (`MISSING_ATTRIBUTES`). The classic data has 296 scalar-subquery comparisons, written as
  `x = df.agg(...).first()[0]`, but never with a `WHERE` inside the subquery.
* v12 dropped on complex pipelines (22/32 vs. v11-q4's 26), so the complex_v9 replay is larger (4,000 vs. 2,500).

```python
# SELECT id, rating, country FROM suppliers
# WHERE rating >= (SELECT MAX(rating) FROM suppliers WHERE supplier_name IS NOT NULL)
max_rating = df_suppliers.filter(F.col("supplier_name").isNotNull()).agg(F.max("rating")).first()[0]

result = df_suppliers.filter(F.col("rating") >= max_rating).select("id", "rating", "country")
```

## What is in it

`scalar_where` (`scw`, from [`../targeted_v11/generate.py`](../targeted_v11/generate.py) `--counts v13`; the v11 / v12
options still reproduce their data byte for byte), 2,000 train: an uncorrelated scalar subquery (usually with its own
`WHERE`, same table or a table with a related numeric column) used as

| Shape | SQL | PySpark |
|---|---|---|
| comparison | `x >= (SELECT AVG(x) FROM t WHERE ...)`, `= (SELECT MAX ...)` | `v = df.filter(...).agg(F.avg("x")).first()[0]`, `.filter(F.col("x") >= v)` |
| arithmetic | `x <= 1.5 * (SELECT AVG(x) ...)` | `.filter(F.col("x") <= F.lit(v) * 1.5)` (`F.lit` keeps an empty subquery a NULL) |
| `BETWEEN` | `x BETWEEN (SELECT MIN ...) AND (SELECT AVG ...)` | two values, `.between(v1, v2)` |
| `SELECT` item | `x - (SELECT AVG(x) ...) AS diff`, `ROUND(x * 100.0 / (SELECT SUM(x) ...), 2) AS pct` | `(F.col("x") - v).alias(...)`, `F.round(F.col("x") * 100.0 / v, 2)` |
| `HAVING` | `GROUP BY g HAVING MAX(x) < (SELECT AVG(x) ...)` | `.groupBy("g").agg(...).filter(F.col("max_x") < v)` |
| join | `child c JOIN parent p ... WHERE c.x >= (SELECT MIN(x) ...)` | join, then `.filter(F.col("c.x") >= v)` |

Spark verification: **2,600 / 2,600 raw pairs match**; 347 empty results are dropped, and no SQL is shared with an eval
set or with targeted_v11 / v12. 150 are held out (`data/heldout_meta.jsonl`), 75 go to validation.

| Path | Content |
|---|---|
| `build.py` | the split above; `data/` and `data_mix/` |
| `data_mix/` | what v13 trains on (11,500, mean ~2,200 characters): 2,000 new + 4,000 complex_v9 + 2,000 targeted_v12 + 1,500 targeted_v11 + 500 classic `OR` + 1,500 other classic; validation 75 new + 300 v12 |
| `lora_config.yaml` | continues from v12 (`../targeted_v12/adapters`), 2,400 micro-batches, writes `targeted_v13/adapters/` |

## Rebuild

```bash
cd sql2pyspark-mlx/targeted_v13
../.venv/bin/python ../targeted_v11/generate.py --seed 13 --counts v13 --out raw_v13    # 2,600 raw pairs
../.venv/bin/python ../verify_pairs.py --src raw_v13 --workers 5 2>/dev/null          # ~3 min
../.venv/bin/python build.py
```

Train, export and evaluate: `zsh eval/start_training_v13.sh` from the repo root.

## Results (v13, 2026-10-03) — regression, not used

Trained 2026-10-03 06:29 → 11:38 (5 h 09 min, peak 10.0 GB), validation loss 0.010 → 0.010 → 0.008 → 0.007
(`train_v13.log`). v13-q4 vs. v12-q4: **150 / 123** of the 150 held-out pairs, `gen01245` / `gen05869` fixed — but hold-out
52 / 59, fixed test 134 / 143 (classic `subquery` family 6 / 15 of 17), v4 hold-out 19 / 21, complex 79 / 83.

The pairs taught "scalar subquery → separate aggregate value", and v13 applied it to *correlated* scalar subqueries
(`... WHERE t2.k = t.k`), which need a window or a grouped join; the mix had no such counter-examples and a small
classic replay. v13 also misspells the unseen hold-out table `patients` as `df_patents`. v12-q4 stays `latest`. A retry
should put correlated and uncorrelated scalar subqueries side by side and replay all classic scalar-subquery pairs.
Details: [README → v13 results](../../README.md#v13-results-2026-10-03--regression-not-used).
