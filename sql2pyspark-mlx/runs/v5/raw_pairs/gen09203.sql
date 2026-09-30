WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        merchant,
        is_flagged,
        UPPER(merchant) AS merchant_upper,
        COALESCE(merchant, 'DIRECT') AS merchant_clean
    FROM transactions
    WHERE amount >= 1300
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        txn_time,
        merchant,
        is_flagged,
        merchant_clean
    FROM base
    QUALIFY ROW_NUMBER() OVER (PARTITION BY merchant ORDER BY txn_time DESC NULLS FIRST, id) = 1
),
with_prev AS (
    -- Previous event time per key
    SELECT *, LAG(txn_time) OVER (PARTITION BY merchant ORDER BY txn_time, id) AS prev_txn_time
    FROM deduped
),
flagged AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_txn_time IS NULL OR txn_time > prev_txn_time + INTERVAL 60 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY merchant ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
prev_events AS (
    -- Previous event time per key
    SELECT *, LAG(prev_txn_time) OVER (PARTITION BY merchant ORDER BY prev_txn_time, id) AS prev_prev_txn_time
    FROM sessionized
),
with_gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_prev_txn_time IS NULL OR prev_txn_time > prev_prev_txn_time + INTERVAL '90' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM prev_events
),
sessionized_transactions AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY merchant ORDER BY prev_txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id_2
    FROM with_gaps
),
flagged_2 AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN txn_time - LAG(txn_time) OVER (PARTITION BY merchant ORDER BY txn_time, id) > INTERVAL 60 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM sessionized_transactions
),
sessions_transactions AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        txn_time,
        merchant,
        prev_txn_time,
        prev_prev_txn_time,
        is_new_burst,
        burst_id_2,
        SUM(session_start_flag) OVER (PARTITION BY merchant ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged_2
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('WEEK', txn_time) AS week_start,
    CASE WHEN session_id >= 7 THEN 'HIGH' ELSE 'LOW' END AS session_id_tier,
    MIN(prev_txn_time) AS first_seen,
    SUM(CASE WHEN merchant = 'Shell' THEN burst_id_2 ELSE 0 END) AS shell_burst_id_2,
    COUNT(CASE WHEN merchant = 'Grab' THEN 1 END) AS grab_count
FROM sessions_transactions
GROUP BY CUBE (DATE_TRUNC('WEEK', txn_time), CASE WHEN session_id >= 7 THEN 'HIGH' ELSE 'LOW' END)
