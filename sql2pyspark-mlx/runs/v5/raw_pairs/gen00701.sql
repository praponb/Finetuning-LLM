WITH with_prev AS (
    -- Previous event time per key
    SELECT *, LAG(txn_time) OVER (PARTITION BY txn_type ORDER BY txn_time, id) AS prev_txn_time
    FROM transactions
),
flagged AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_txn_time IS NULL OR txn_time > prev_txn_time + INTERVAL 30 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM with_prev
),
islands AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        txn_type,
        amount,
        txn_time,
        merchant,
        is_flagged,
        SUM(is_new_burst) OVER (PARTITION BY txn_type ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
)
-- Multi-level aggregation
SELECT
    CASE WHEN session_id >= 8 THEN 'HIGH' ELSE 'LOW' END AS session_id_tier,
    merchant,
    COUNT(*) AS cnt,
    ROUND(SUM(amount) / NULLIF(COUNT(CASE WHEN txn_type = 'CREDIT' THEN 1 END), 0), 2) AS amount_per_credit,
    MAX(txn_time) AS ended_at,
    MAX(txn_time) AS last_txn_time,
    COUNT(DISTINCT txn_type) AS distinct_txn_types,
    (UNIX_TIMESTAMP(MAX(txn_time)) - UNIX_TIMESTAMP(MIN(txn_time))) / 60 AS duration_min
FROM islands
GROUP BY session_id_tier, merchant
ORDER BY distinct_txn_types DESC;
