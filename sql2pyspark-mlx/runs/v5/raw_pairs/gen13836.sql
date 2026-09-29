WITH base AS (
    -- Basic cleansing and defaults
    SELECT *, UPPER(merchant) AS merchant_upper
    FROM transactions
    WHERE txn_time >= DATE '2023-10-04'
      AND merchant IS DISTINCT FROM 'Lazada'
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(txn_time) OVER (PARTITION BY account_id ORDER BY txn_time, id) IS NULL
                 OR txn_time > LAG(txn_time) OVER (PARTITION BY account_id ORDER BY txn_time, id) + INTERVAL '7' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM base
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY account_id ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM gaps
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('QUARTER', txn_time) AS quarter_start,
    txn_type,
    PERCENTILE_APPROX(session_no, 0.9) AS session_no_p90,
    ROUND(SUM(amount) / NULLIF(COUNT(CASE WHEN txn_type = 'REFUND' THEN 1 END), 0), 2) AS amount_per_refund,
    SIZE(COLLECT_SET(merchant)) AS num_merchants,
    GROUPING(DATE_TRUNC('QUARTER', txn_time)) AS gid
FROM islands
GROUP BY ROLLUP (DATE_TRUNC('QUARTER', txn_time), txn_type)
