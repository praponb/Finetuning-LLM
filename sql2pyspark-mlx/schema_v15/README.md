# schema_v15 — the existing query forms on synthetic schemas with unseen names (v15)

Training data for **v15**, which continues from **v14** ([targeted_v14](../targeted_v14/README.md)).

## Why

Every dataset up to v14 used the same **42 tables** (17 classic + 5 nested + 20 complex_v8) with **191 column names**,
and each targeted dataset added thousands more pairs on them. v13 and v14 then started to recall training names instead
of copying the names in the SQL, most of all where an unseen name looks like a familiar one:

| Unseen name | Model wrote | Familiar training name |
| --- | --- | --- |
| `appt_date` (hold-out) | `apppt_date` | — |
| `num_stops`, `long_wait_stops` (v4 hold-out) | `num_stores`, `long_wait_stores` | `stores` |
| `patients` (hold-out, v13) | `df_patents` | — |
| `ss.revenue` (fixed test) | `ss.restock` | `last_restock` |

These pairs keep the query forms and change every name, so copying the names from the SQL is the only strategy that
works.

## What is in it

[`synth_catalog.py`](synth_catalog.py): **35 domains, 221 tables, 759 distinct column names** (energy, schools, farms,
gaming, telecom, insurance, realty, sports, library, museums, airports, labs, shipping, transit, …), each domain 4–7
tables linked by foreign keys plus one nested table (an `ARRAY<STRUCT<...>>` and a `STRUCT` column with domain field
names). Name styles vary: `dim_` / `fact_` / `tbl_` prefixes, per-table abbreviations (`cnt` / `count`, `amt` /
`amount`, `pct` / `percent`, …), fk columns named `<parent>_id`, `_ref` or `_no`, and near neighbours of training
names on purpose (`store_kiosks`, `restock_qty`, `revenue_share`, `stop_seq`, `stop_label`). Identifiers of the hold-out
and real-world query tables that no training table uses are forbidden, and no table repeats a training table name
(checked at import). Rows: `schemas.make_rows` (40 seeded rows) or a nested-row function.

[`patch.py`](patch.py) puts the catalog into `schemas` before the generators are imported, so the existing generators
run unchanged on it; [`generate.py`](generate.py) produces 5 groups (the code is rewritten to the inline "mirror" window
style with [classic_v10/mirror.py](../classic_v10/mirror.py)):

| Group | Source | Train |
| --- | --- | --- |
| `classic` | `generate_dataset.py` families (select, aggregation, joins, windows, top-N, CTEs, subqueries, set operations, special) | 4,000 |
| `adv` | `advanced.py` flat-table families (multi-stage pipelines, dedup, sessionize, reshape, advanced windows / joins / set operations) | 1,000 |
| `nested` | [`nested_builder.py`](nested_builder.py): `SIZE` / `TRANSFORM` / `FILTER` / `EXISTS` / `FORALL` / `AGGREGATE` / `ARRAY_MAX` over an array of structs, struct fields, `LATERAL VIEW [OUTER] EXPLODE` | 1,000 |
| `subq` | `../targeted_v11/generate.py`: correlated `[NOT] EXISTS` / `IN`, two subqueries, `GROUP BY` after a subquery, set operations, `OR` (`or_where` + `OR` inside the others at rate 0.3) | 2,000 |
| `scalar` | `../targeted_v14/generate.py`: uncorrelated / correlated / child-table / mixed scalar subqueries | 2,000 |

40 validation pairs per group. **Schema-shift eval set** (`data/schema_shift_meta.jsonl`, 200 pairs: 90 / 20 / 30 /
30 / 30): the same groups on the 5 domains in `HELDOUT_DOMAINS` (pets, space, charity, auto_dealer, water_utility),
which training never sees; evaluate with `--tables eval/schema_shift_tables.py`.

| Path | Content |
| --- | --- |
| `synth_catalog.py`, `patch.py`, `nested_builder.py`, `generate.py` | the catalog and the pair generators |
| `verify.py` | `../verify_pairs.py` with the catalog installed in every worker |
| `build.py` | `data/` (train / valid / `schema_shift_meta.jsonl`) and `data_mix/` |
| `data_mix/` | what v15 trains on (~25,000): 10,000 new + 3,000 targeted_v14 + all 708 classic scalar-subquery pairs + 4,800 classic + 500 classic `OR` + 3,500 complex_v9 + 1,500 targeted_v12 + 1,000 targeted_v11 |
| `lora_config.yaml` | continues from v14 (`../targeted_v14/adapters`), 4,800 micro-batches, writes `schema_v15/adapters/` |

## Rebuild

```bash
cd sql2pyspark-mlx/schema_v15
../.venv/bin/python generate.py --split train --out raw_v15            # 14,000 raw pairs
../.venv/bin/python generate.py --split heldout --out raw_v15_shift    # 340 raw pairs
../.venv/bin/python verify.py --src raw_v15 --workers 5 2>/dev/null
../.venv/bin/python verify.py --src raw_v15_shift --workers 5 2>/dev/null
../.venv/bin/python build.py
```

Train, export and evaluate: `zsh eval/start_training_v15.sh` from the repo root (it also scores v14-q4 and v12-q4 on
the schema-shift set and prints `eval/name_slips.py`, which counts failures with an unresolved name).

## Results (v15, 2026-10-08) — `latest` on the Mac since 2026-10-09

Trained 2026-10-07 13:18 → 21:27 (8 h 09 min, peak 10.0 GB), validation loss 0.011 → 0.019 → 0.009 → 0.008 → 0.007 →
0.007 → 0.007 (`train_v15.log`). v15-q4 vs. v14-q4 / v12-q4:

* **Schema-shift set 189 / 179 / 155** of 200 (nested 30/30), failures with an unresolved name across all eval sets
  **42 / 57 / 93**, hold-out **60** / 58 / 59.
* Kept or better: classic families 97/97 (`subquery` 17/17), fixed test 145, complex **89** (set operations 22/25,
  pipelines 29/32), `IN` / `NOT IN` 50/50, v12 / v13 held-out 232 / 150.
* Costs: v4 hold-out 20 (v12-q4 21), v11 held-out 278 (287).

Details: [README → v15 results](../../README.md#v15-results-2026-10-08).
