WITH with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(txn_time) OVER (PARTITION BY merchant ORDER BY txn_time, id) IS NULL
                 OR txn_time > LAG(txn_time) OVER (PARTITION BY merchant ORDER BY txn_time, id) + INTERVAL '30' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM transactions
),
sessions_transactions AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY merchant ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM with_gaps
)
-- Multi-level aggregation
SELECT
    CASE WHEN new_group_flag >= 1 THEN 'LARGE' ELSE 'SMALL' END AS new_group_flag_tier,
    DATE_TRUNC('WEEK', txn_time) AS week_start,
    SUM(CASE WHEN txn_type = 'CREDIT' THEN island_id ELSE 0 END) AS credit_island_id,
    (UNIX_TIMESTAMP(MAX(txn_time)) - UNIX_TIMESTAMP(MIN(txn_time))) / 60 AS duration_min,
    (UNIX_TIMESTAMP(MAX(txn_time)) - UNIX_TIMESTAMP(MIN(txn_time))) / 60 AS duration_min_2,
    SORT_ARRAY(COLLECT_SET(txn_type)) AS txn_type_list
FROM sessions_transactions
GROUP BY CUBE (CASE WHEN new_group_flag >= 1 THEN 'LARGE' ELSE 'SMALL' END, DATE_TRUNC('WEEK', txn_time))
HAVING duration_min_2 < 1000000
ORDER BY credit_island_id NULLS FIRST;
