WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        merchant,
        CASE WHEN amount >= 4100 THEN 'High' WHEN amount >= 600 THEN 'Medium' ELSE 'Low' END AS amount_tier
    FROM transactions
    WHERE NOT is_flagged
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(txn_time) OVER (PARTITION BY merchant ORDER BY txn_time ASC, id ASC) IS NULL
                 OR txn_time > LAG(txn_time) OVER (PARTITION BY merchant ORDER BY txn_time ASC, id ASC) + INTERVAL '7' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM valid
),
sessions_transactions AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY merchant ORDER BY txn_time ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM with_gaps
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('YEAR', txn_time) AS year_start,
    APPROX_PERCENTILE(session_start_flag, ARRAY(0.25, 0.5, 0.75)) AS session_start_flag_quartiles,
    COUNT(*) AS num_rows,
    SUM(session_no) AS total_session_no
FROM sessions_transactions
GROUP BY ROLLUP (DATE_TRUNC('YEAR', txn_time))
ORDER BY year_start ASC NULLS LAST;
