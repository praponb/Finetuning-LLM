WITH with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN txn_time - LAG(txn_time) OVER (PARTITION BY merchant ORDER BY txn_time, id) > INTERVAL '60' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM transactions
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY merchant ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM with_gaps
)
-- Grouped metrics
SELECT
    burst_id,
    ROUND(AVG(amount), 2) AS avg_amount,
    SORT_ARRAY(COLLECT_SET(merchant)) AS merchant_list,
    MIN(amount) AS min_amount
FROM sessionized
GROUP BY burst_id
HAVING MIN(amount) >= 0
ORDER BY avg_amount;
