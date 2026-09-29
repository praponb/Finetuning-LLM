WITH latest AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY amount DESC NULLS FIRST, id) AS dedup_rank
    FROM transactions
    QUALIFY dedup_rank = 1
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        txn_time,
        merchant,
        dedup_rank,
        CUME_DIST() OVER (PARTITION BY account_id, merchant ORDER BY amount DESC) AS amount_cume_dist,
        AVG(amount) OVER (PARTITION BY merchant) AS merchant_avg_amount,
        LAG(amount, 1) OVER w_running AS prev_amount,
        SUM(amount) OVER (PARTITION BY merchant ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_amount
    FROM latest
    WINDOW w_running AS (PARTITION BY account_id ORDER BY amount, id)
),
with_prev AS (
    -- Previous event time per key
    SELECT *, LAG(txn_time) OVER (PARTITION BY merchant ORDER BY txn_time, id) AS prev_txn_time
    FROM with_windows
),
with_gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_txn_time IS NULL OR txn_time > prev_txn_time + INTERVAL '90' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY merchant ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM with_gaps
),
with_windows_transactions AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        txn_time,
        prev_amount,
        running_amount,
        prev_txn_time,
        session_start_flag,
        STDDEV_SAMP(session_no) OVER w_grp AS merchant_stddev_samp_session_no,
        SUM(session_start_flag) OVER (PARTITION BY merchant ORDER BY prev_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_session_start_flag
    FROM sessionized
    WHERE merchant IN ('Shell', 'Starbucks')
    WINDOW w_grp AS (PARTITION BY merchant)
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        NTILE(4) OVER (ORDER BY prev_amount DESC, id) AS prev_amount_bucket,
        RANK() OVER (ORDER BY prev_amount NULLS LAST) AS prev_amount_rank
    FROM with_windows_transactions
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(prev_amount_rank) OVER w_ordered AS running_sum_prev_amount_rank,
        COUNT(prev_amount_rank) OVER w_ordered AS running_count_prev_amount_rank,
        MIN(prev_amount_rank) OVER w_ordered AS running_min_prev_amount_rank,
        LAST_VALUE(prev_amount) IGNORE NULLS OVER (ORDER BY prev_amount_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_prev_amount
    FROM metrics
    WINDOW w_ordered AS (ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
-- Ranking and gap features
SELECT
    id,
    txn_time,
    running_amount,
    prev_txn_time,
    merchant_stddev_samp_session_no,
    running_session_start_flag,
    prev_amount_bucket,
    running_sum_prev_amount_rank,
    LEAD(running_amount) OVER (ORDER BY running_count_prev_amount_rank, id) AS next_running_amount,
    LAG(running_amount) OVER (ORDER BY running_count_prev_amount_rank, id) AS prev_running_amount,
    LEAD(last_known_prev_amount) OVER (ORDER BY running_count_prev_amount_rank, id) AS next_last_known_prev_amount,
    LAG(last_known_prev_amount) OVER (ORDER BY running_count_prev_amount_rank, id) AS prev_last_known_prev_amount,
    ROW_NUMBER() OVER (ORDER BY running_count_prev_amount_rank, id) AS seq_no,
    LAG(last_known_prev_amount, 1) OVER (ORDER BY running_min_prev_amount_rank, id) AS prev_last_known_prev_amount_2
FROM enriched;
