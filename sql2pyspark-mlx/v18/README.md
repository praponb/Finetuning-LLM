# v18 — two ways to combine v15 and v17 (2026-11-15): v18a-q4 is `latest`

v17 beat v15 on hold-out and complex-shift but lost on fixed / complex, and its per-case diff with v15 was scattered
churn, so v18 tried two ways to keep both models' strengths. Chain: [`eval/start_training_v18.sh`](../../eval/start_training_v18.sh)
(log `eval/start_training_v18.log`).

- **v18avg** ([`average.py`](average.py)): the exact average of the v15 and v17 LoRA deltas, built by concatenating the
  two adapters on the rank axis (lora_a stacked, lora_b × 0.5) → a rank-32 adapter in `adapters_avg/` (rel. error
  1.4e-7). No training.
- **v18a** ([`lora_config.yaml`](lora_config.yaml), log `train_v18a.log`): v15's adapter continued on the v17 mix
  (`complex_v17/data_mix`), 2,400 micro-batches, lr 2e-5 → 2e-6 (cosine), seed 18 → `adapters_a/`. Training
  2026-11-15 03:35 → 05:07 on the Mac mini, val loss 0.009 → 0.005.

| Eval set | v18avg-q4 | **v18a-q4** | v15-q4 | v17-q4 |
| --- | --- | --- | --- | --- |
| Hold-out (62) | **61** | **61** | 60 | **61** |
| v4 hold-out (25) | 20 | 20 | 20 | **21** |
| Fixed test (150) | 143 | **145** | **145** | 142 |
| complex_v8 test (100) | 85 | 88 | **89** | 85 |
| `IN` / `NOT IN` (50) | 50 | 50 | 50 | 50 |
| targeted_v11 / v12 held-out | **285** / **236** | 281 / 232 | 278 / 232 | 283 / 233 |
| targeted_v13 / v14 held-out | **150** / 195 | 148 / 195 | **150** / 193 | **150** / **197** |
| Schema-shift (200) | **190** | 186 | 189 | 189 |
| Complex schema-shift (150) | 110 | **126** | 110 | 116 |
| Failures / with a name slip | 112 / 50 | **105** / 53 | 121 / 62 | 110 / 58 |

v18a misses the rule only on the complex test, by one query (88 vs. 89), and is the best overall (complex-shift +16,
fewest failures). The owner **promoted v18a-q4 on 2026-11-15 ~12:20 Bangkok** (`ollama cp sql2pyspark:v18a-q4
sql2pyspark:latest` on the Mac mini; `latest` = `03bb9f945dd4`, was `882787d22c3c` = v15-q4); the root GGUFs are v18a /
v18a-q4. Rollback: `ollama cp sql2pyspark:v15-q4 sql2pyspark:latest`.
