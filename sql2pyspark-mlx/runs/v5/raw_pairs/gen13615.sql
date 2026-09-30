WITH flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN txn_time - LAG(txn_time) OVER (PARTITION BY account_id ORDER BY txn_time, id) > INTERVAL '14' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM transactions
),
islands AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        SUM(session_start_flag) OVER (PARTITION BY account_id ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
)
-- Multi-level aggregation
SELECT
    CASE WHEN burst_id >= 7 THEN 'HIGH' ELSE 'LOW' END AS burst_id_tier,
    account_id,
    burst_id,
    COUNT(DISTINCT id) AS distinct_ids,
    (UNIX_TIMESTAMP(MAX(txn_time)) - UNIX_TIMESTAMP(MIN(txn_time))) / 60 AS duration_min
FROM islands
GROUP BY CASE WHEN burst_id >= 7 THEN 'HIGH' ELSE 'LOW' END, account_id, burst_id
ORDER BY burst_id DESC
