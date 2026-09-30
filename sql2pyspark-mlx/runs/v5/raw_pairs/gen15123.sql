WITH flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN txn_time - LAG(txn_time) OVER (PARTITION BY merchant ORDER BY txn_time, id) > INTERVAL 30 DAYS THEN 1 ELSE 0 END AS new_group_flag
    FROM transactions
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY merchant ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM islands
    QUALIFY ROW_NUMBER() OVER (PARTITION BY txn_type ORDER BY txn_time DESC, id) = 1
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        txn_type,
        txn_time,
        merchant,
        LAG(burst_id) OVER (PARTITION BY merchant, txn_type ORDER BY txn_time, id) prev_burst_id,
        LEAD(burst_id) OVER (PARTITION BY merchant, txn_type ORDER BY txn_time, id) next_burst_id,
        LEAD(new_group_flag) OVER (PARTITION BY merchant, txn_type ORDER BY txn_time, id) next_new_group_flag,
        LAG(new_group_flag) OVER (PARTITION BY merchant, txn_type ORDER BY txn_time, id) prev_new_group_flag,
        ROW_NUMBER() OVER (PARTITION BY merchant, txn_type ORDER BY txn_time, id) seq_no,
        LEAD(burst_id, 2) OVER (PARTITION BY account_id, txn_type ORDER BY amount, id) next_burst_id_2
    FROM deduped
),
deduped_transactions AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM enriched
    QUALIFY ROW_NUMBER() OVER (PARTITION BY merchant, txn_type ORDER BY prev_new_group_flag NULLS LAST, id) = 1
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        txn_type,
        merchant,
        next_burst_id,
        next_new_group_flag,
        prev_new_group_flag,
        seq_no,
        next_burst_id_2,
        GREATEST(next_burst_id, seq_no) greatest_next_burst_id_seq_no
    FROM deduped_transactions
    WHERE txn_time > DATE '2023-06-07'
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN seq_no >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END AS seq_no_tier,
    txn_type,
    MAX(seq_no) FILTER (WHERE merchant = 'Grab') AS grab_max_seq_no,
    COUNT(DISTINCT txn_type) AS distinct_txn_types,
    COUNT_IF(txn_type = 'REFUND') AS refund_events,
    SUM(CASE WHEN txn_type = 'REFUND' THEN prev_new_group_flag ELSE 0 END) AS refund_prev_new_group_flag,
    APPROX_PERCENTILE(prev_new_group_flag, 0.9) AS prev_new_group_flag_p90,
    SIZE(COLLECT_SET(merchant)) AS num_merchants
FROM filtered
GROUP BY CASE WHEN seq_no >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END, txn_type
ORDER BY prev_new_group_flag_p90 DESC NULLS FIRST;
