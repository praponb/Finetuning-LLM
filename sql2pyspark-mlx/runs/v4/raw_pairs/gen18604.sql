WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        DAY(txn_time) AS txn_time_day,
        COALESCE(merchant, 'NONE') AS resolved_merchant
    FROM transactions
    WHERE txn_time > TO_TIMESTAMP('2023-07-04 22:00:00')
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(txn_time) OVER (PARTITION BY merchant ORDER BY txn_time, id) IS NULL
                 OR txn_time > LAG(txn_time) OVER (PARTITION BY merchant ORDER BY txn_time, id) + INTERVAL 14 DAYS THEN 1 ELSE 0 END AS new_group_flag
    FROM cleaned
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY merchant ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged
)
-- Grouped metrics
SELECT
    DATE_TRUNC('WEEK', txn_time) AS week_start,
    COUNT(CASE WHEN merchant = 'Shell' THEN 1 END) AS shell_count,
    SUM(CASE WHEN txn_type = 'FEE' THEN new_group_flag ELSE 0 END) AS fee_new_group_flag,
    COUNT(*) FILTER (WHERE merchant = 'Lazada') AS lazada_rows,
    GROUPING(DATE_TRUNC('WEEK', txn_time)) AS grouping_id
FROM sessionized
GROUP BY ROLLUP (DATE_TRUNC('WEEK', txn_time))
