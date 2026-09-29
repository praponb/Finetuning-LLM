WITH gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN txn_time - LAG(txn_time) OVER (PARTITION BY account_id ORDER BY txn_time, id) > INTERVAL '30' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM transactions
)
-- Running sum of the flag = session number
SELECT
    id,
    account_id,
    txn_type,
    txn_time,
    merchant,
    is_flagged,
    SUM(session_start_flag) OVER (PARTITION BY account_id ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
FROM gaps;
