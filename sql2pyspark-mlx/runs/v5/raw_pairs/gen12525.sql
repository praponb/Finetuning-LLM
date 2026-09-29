WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        DATE_TRUNC('WEEK', txn_time) AS txn_time_period,
        CAST(txn_time AS DATE) AS txn_time_day
    FROM transactions
    WHERE is_flagged = TRUE
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN txn_time_period - LAG(txn_time_period) OVER (PARTITION BY txn_type ORDER BY txn_time_period, id) > INTERVAL 30 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM filtered
),
sessions_transactions AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY txn_type ORDER BY txn_time_period, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('WEEK', txn_time_day) AS week_start,
    burst_id,
    SORT_ARRAY(COLLECT_SET(merchant)) AS merchant_list,
    ROUND(AVG(burst_id), 2) AS avg_burst_id,
    COUNT(*) AS num_rows,
    COUNT_IF(merchant = 'Amazon') AS amazon_events
FROM sessions_transactions
GROUP BY ROLLUP (DATE_TRUNC('WEEK', txn_time_day), burst_id)
ORDER BY num_rows DESC NULLS LAST;
