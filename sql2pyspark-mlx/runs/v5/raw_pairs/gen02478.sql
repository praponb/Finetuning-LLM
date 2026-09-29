WITH flagged AS (
    SELECT *, CASE WHEN LAG(txn_time) OVER (PARTITION BY merchant ORDER BY txn_time, id) IS NULL
                 OR txn_time > LAG(txn_time) OVER (PARTITION BY merchant ORDER BY txn_time, id) + INTERVAL '60' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM transactions
),
islands AS (
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY merchant ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
)
SELECT
    id,
    txn_time,
    merchant,
    is_flagged,
    new_group_flag,
    burst_id
FROM islands
ORDER BY txn_time DESC
