WITH gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(txn_time) OVER (PARTITION BY account_id ORDER BY txn_time, id) IS NULL
                 OR txn_time > LAG(txn_time) OVER (PARTITION BY account_id ORDER BY txn_time, id) + INTERVAL 14 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM transactions
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY account_id ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM gaps
)
-- Aggregate with conditional metrics
SELECT
    account_id,
    CASE WHEN session_no >= 8 THEN 'HIGH' ELSE 'LOW' END AS session_no_tier,
    txn_type,
    ROUND(SUM(session_start_flag) / NULLIF(COUNT(CASE WHEN merchant = 'Lazada' THEN 1 END), 0), 2) AS session_start_flag_per_lazada,
    MAX(txn_time) AS ended_at,
    MIN(txn_time) AS first_txn_time
FROM islands
GROUP BY GROUPING SETS ((account_id, CASE WHEN session_no >= 8 THEN 'HIGH' ELSE 'LOW' END, txn_type), (account_id, CASE WHEN session_no >= 8 THEN 'HIGH' ELSE 'LOW' END), (account_id))
ORDER BY session_start_flag_per_lazada NULLS FIRST;
