WITH base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        ROUND(amount * 1.07, 2) amount_adjusted,
        CONCAT_WS(',', merchant, txn_type) merchant_txn_type_key
    FROM transactions
    WHERE txn_time >= DATE '2023-09-27'
),
enriched AS (
    -- Attach accounts attributes
    SELECT
        s.id,
        s.account_id,
        s.txn_type,
        s.amount,
        s.txn_time,
        s.is_flagged,
        s.amount_adjusted,
        b.status
    FROM base s
    INNER JOIN accounts b
        ON s.account_id = b.id
    WHERE b.opened_date <= '2025-05-01'
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN txn_time - LAG(txn_time) OVER (PARTITION BY txn_type ORDER BY txn_time, id) > INTERVAL '30' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM enriched
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        account_id,
        txn_type,
        txn_time,
        amount_adjusted,
        status,
        SUM(is_new_burst) OVER (PARTITION BY txn_type ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM gaps
),
enriched_transactions AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        txn_type,
        txn_time,
        amount_adjusted,
        MAX(burst_id) OVER (PARTITION BY txn_type) txn_type_max_burst_id,
        ROW_NUMBER() OVER (PARTITION BY txn_type, account_id ORDER BY txn_time DESC, id) event_seq,
        LEAD(amount_adjusted, 1) OVER (PARTITION BY status ORDER BY txn_time, id) next_amount_adjusted
    FROM sessionized
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY txn_type ORDER BY event_seq DESC, id) AS latest_rank
    FROM enriched_transactions
    QUALIFY latest_rank = 1
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        LEAD(amount_adjusted) OVER (PARTITION BY txn_type ORDER BY txn_time, id) next_amount_adjusted_2,
        LAG(amount_adjusted) OVER (PARTITION BY txn_type ORDER BY txn_time, id) prev_amount_adjusted,
        LAG(txn_type_max_burst_id) OVER (PARTITION BY txn_type ORDER BY txn_time, id) prev_txn_type_max_burst_id,
        LEAD(txn_type_max_burst_id) OVER (PARTITION BY txn_type ORDER BY txn_time, id) next_txn_type_max_burst_id,
        ROW_NUMBER() OVER (PARTITION BY txn_type ORDER BY txn_time, id) seq_no
    FROM ranked
),
enriched_transactions_2 AS (
    -- Ranking and gap features
    SELECT
        *,
        COUNT(prev_amount_adjusted) OVER w_part running_count_prev_amount_adjusted,
        MIN(prev_amount_adjusted) OVER w_part running_min_prev_amount_adjusted,
        SUM(prev_amount_adjusted) OVER w_part running_sum_prev_amount_adjusted,
        AVG(next_amount_adjusted_2) OVER (PARTITION BY txn_type ORDER BY next_amount_adjusted_2, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) next_amount_adjusted_2_avg_6
    FROM with_windows
    WINDOW w_part AS (PARTITION BY txn_type ORDER BY event_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT
    id,
    txn_type,
    txn_time,
    txn_type_max_burst_id,
    next_amount_adjusted,
    next_amount_adjusted_2,
    prev_amount_adjusted,
    prev_txn_type_max_burst_id,
    seq_no
FROM enriched_transactions_2
ORDER BY seq_no DESC NULLS FIRST
