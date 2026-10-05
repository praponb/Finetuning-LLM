# targeted_v12 — OR / mixed AND-OR conditions (+ replay), the follow-up to targeted_v11

Training data for **v12**, the follow-up to v11 ([targeted_v11](../targeted_v11/README.md)).

## Why

v11 learned the subquery rule (283/300 held-out targeted pairs), but on 2 fixed-test queries it split
`WHERE a OR b` into two chained `.filter()` calls, which means AND and returns wrong rows without an error
(`gen10459`, `gen11114`; classic families 93/97). Its 10,000 new pairs always wrote one `.filter()` per condition and
contained no `OR`; only 405 of the 15,000 mix samples had one.

```python
# SELECT id, name FROM customers WHERE country = 'TH' OR credit_limit > 5000
result = df_customers.filter((F.col("country") == "TH") | (F.col("credit_limit") > 5000)).select("id", "name")
```

## What is in it

The pairs come from [`../targeted_v11/generate.py`](../targeted_v11/generate.py) with `--counts v12 --or-rate 0.5`
(the default options still reproduce targeted_v11 byte for byte):

| Family | Train | What |
|---|---|---|
| `or_where` (`orw`, new) | 2,000 | one table, `child JOIN parent` or `GROUP BY` with a `WHERE` mixing `OR` / `AND` / `NOT`: `a OR b`, `(a AND b) OR c`, `a AND b OR c`, `a OR b AND c` (AND binds tighter), `a AND (b OR c)`, `NOT (a OR b)`, `a OR b OR c` → one `.filter()` with `|` / `&` / `~` |
| the six targeted_v11 families | 2,700 | as in v11 (1,000 / 300 / 400 / 400 / 300 / 300), but half of their filters are an `OR` of two conditions, and some subquery cross conditions are OR'ed with an inner condition (`(o.amount > s.sqft OR o.status IN (...))`): such a mixed `OR` references the outer query, so it stays in the **join condition** |

Spark verification: **6,950 / 6,950 raw pairs match**; 597 empty results are dropped, and no SQL is shared with an
eval set or with targeted_v11. 3,681 of the 4,700 new training pairs contain `OR`.

| Path | Content |
|---|---|
| `build.py` | per family: 100 (`or_where`) / 25 held out (`data/heldout_meta.jsonl`, 250), 25 / 10 validation, the train quota; writes `data/` and `data_mix/` |
| `data_mix/` | what v12 trains on (12,976): 4,700 new + all 976 classic v7 training pairs with `OR` + 2,800 other classic + 2,500 complex_v9 + 2,000 targeted_v11 (keeps the subquery rule); 4,982 contain `OR`; validation 85 new + 300 v11 |
| `lora_config.yaml` | continues from v11 (`../targeted_v11/adapters`), 2,400 micro-batches, writes `targeted_v12/adapters/` |

## Rebuild

```bash
cd sql2pyspark-mlx/targeted_v12
../.venv/bin/python ../targeted_v11/generate.py --seed 12 --counts v12 --or-rate 0.5 --out raw_v12   # 6,950 raw pairs
../.venv/bin/python ../verify_pairs.py --src raw_v12 --workers 5 2>/dev/null                         # ~5 min
../.venv/bin/python build.py
```

Train, export and evaluate: `zsh eval/start_training_v12.sh` from the repo root.

## Results (v12, 2026-10-03)

Trained 2026-10-02 18:55 → 23:47 (4 h 52 min, peak 9.9 GB), validation loss 0.012 → 0.014 → 0.008 → **0.007**
(`train_v12.log`).

| Eval set | v9-q4 (server until 10-03) | v11-q4 (`latest` 10-02 → 10-03) | v12-q4 |
|---|---|---|---|
| Held-out targeted_v12 pairs (250, `data/heldout_meta.jsonl`) | 148 | 169 | **230** |
| … `or_where` (100) | **98** | 79 | 97 |
| Fixed-test queries with `OR` (12) | **11** | 9 | **11** |
| Held-out targeted_v11 pairs (300) | 130 | 283 | **287** |
| Fixed test (150) / classic (97) | 140 / **97** | 143 / 93 | 143 / 95 |
| v4 hold-out (25) / hold-out (62) | 21 / 59 | 21 / **60** | 21 / 59 |
| complex_v8 test (100) | **87** | 84 | 83 |

The `OR` slip is gone and v11's subquery gains stay; the two remaining classic misses are uncorrelated scalar
subqueries in `WHERE`. Details: [README → v12 results](../../README.md#v12-results-2026-10-03).
