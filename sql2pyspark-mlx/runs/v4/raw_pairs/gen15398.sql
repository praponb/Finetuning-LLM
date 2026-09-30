WITH gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(txn_time) OVER (PARTITION BY txn_type ORDER BY txn_time, id) IS NULL
                 OR txn_time > LAG(txn_time) OVER (PARTITION BY txn_type ORDER BY txn_time, id) + INTERVAL '7' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM transactions
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY txn_type ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM gaps
)
-- Aggregate with conditional metrics
SELECT
    txn_type,
    DATE_TRUNC('WEEK', txn_time) AS week_start,
    account_id,
    SORT_ARRAY(COLLECT_SET(txn_type)) AS txn_type_list,
    MIN(txn_time) AS first_seen,
    SORT_ARRAY(COLLECT_SET(txn_type)) AS txn_type_list_2,
    MIN(txn_time) AS started_at,
    ROUND(AVG(amount), 2) AS avg_amount
FROM sessionized
GROUP BY txn_type, DATE_TRUNC('WEEK', txn_time), account_id
HAVING ROUND(AVG(amount), 2) > 0
ORDER BY week_start DESC;
