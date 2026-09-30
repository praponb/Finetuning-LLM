WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CONCAT(merchant, '/', txn_type) merchant_txn_type,
        LENGTH(merchant) merchant_length
    FROM transactions
    WHERE txn_time > TO_DATE('2023-09-28')
      AND amount IS NOT NULL
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN txn_time - LAG(txn_time) OVER (PARTITION BY merchant ORDER BY txn_time, id) > INTERVAL 7 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM cleaned
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY merchant ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM gaps
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('WEEK', txn_time) AS week_start,
    MAX(txn_time) AS last_txn_time,
    APPROX_PERCENTILE(merchant_length, ARRAY(0.25, 0.5, 0.75)) AS merchant_length_quartiles,
    COUNT(*) AS cnt,
    SUM(CASE WHEN txn_type = 'DEBIT' THEN amount ELSE 0 END) AS debit_amount
FROM islands
GROUP BY week_start;
