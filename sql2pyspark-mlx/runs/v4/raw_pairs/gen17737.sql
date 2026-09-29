WITH prev_events AS (
    -- Previous event time per key
    SELECT *, LAG(txn_time) OVER (PARTITION BY merchant ORDER BY txn_time, id) AS prev_txn_time
    FROM transactions
),
flagged AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_txn_time IS NULL OR txn_time > prev_txn_time + INTERVAL '90' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM prev_events
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY merchant ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
)
SELECT
    id,
    account_id,
    txn_type,
    merchant,
    is_flagged,
    is_new_burst,
    CASE WHEN merchant RLIKE '^(Lazada|Shell)$' THEN 'CORE' WHEN merchant RLIKE '^(Grab|7-Eleven)$' THEN 'OTHER_ZONE' ELSE 'GROUP_A' END AS merchant_zone
FROM islands
ORDER BY account_id
