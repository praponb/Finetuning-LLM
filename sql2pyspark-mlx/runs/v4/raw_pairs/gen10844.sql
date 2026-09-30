WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        is_flagged,
        UPPER(merchant) AS merchant_upper,
        COALESCE(merchant, 'N/A') AS merchant_clean
    FROM transactions
    WHERE txn_time > TO_TIMESTAMP('2023-05-22 10:00:00')
      AND merchant IS NOT NULL
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN txn_time - LAG(txn_time) OVER (PARTITION BY txn_type ORDER BY txn_time, id) > INTERVAL 30 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM base
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY txn_type ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM with_gaps
)
-- Grouped metrics
SELECT
    CASE WHEN session_start_flag >= 1 THEN 'HIGH' ELSE 'LOW' END AS session_start_flag_tier,
    COUNT(*) FILTER (WHERE txn_type = 'CREDIT') AS credit_rows,
    COUNT(DISTINCT id) AS distinct_ids,
    MIN(session_start_flag) AS min_session_start_flag
FROM islands
GROUP BY 1;
