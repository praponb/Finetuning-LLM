WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        merchant,
        CAST(txn_time AS DATE) txn_time_day
    FROM transactions
    WHERE merchant IS NOT NULL
      AND NOT is_flagged
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(txn_time) OVER (PARTITION BY txn_type ORDER BY txn_time, id) IS NULL
                 OR txn_time > LAG(txn_time) OVER (PARTITION BY txn_type ORDER BY txn_time, id) + INTERVAL '60' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM filtered
),
sessions_transactions AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        merchant,
        SUM(new_group_flag) OVER (PARTITION BY txn_type ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM with_gaps
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('MONTH', txn_time) AS activity_month,
    COUNT(*) AS row_count,
    STDDEV_SAMP(amount) AS amount_stddev
FROM sessions_transactions
GROUP BY DATE_TRUNC('MONTH', txn_time)
HAVING amount_stddev >= 0
