WITH base AS (
    -- Basic cleansing and defaults
    SELECT *, UPPER(merchant) AS merchant_upper
    FROM transactions
    WHERE txn_time >= TIMESTAMP '2023-09-26 13:00:00'
      AND merchant IS NOT NULL
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN txn_time - LAG(txn_time) OVER (PARTITION BY merchant ORDER BY txn_time, id) > INTERVAL 90 DAYS THEN 1 ELSE 0 END AS new_group_flag
    FROM base
),
islands AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        txn_time,
        merchant,
        SUM(new_group_flag) OVER (PARTITION BY merchant ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM with_gaps
),
filtered AS (
    -- Keep recent, valid rows
    SELECT *, TO_DATE(txn_time) AS txn_time_date
    FROM islands
    WHERE merchant IS NOT NULL
),
with_gaps_2 AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(txn_time) OVER (PARTITION BY merchant ORDER BY txn_time, id) IS NULL
                 OR txn_time > LAG(txn_time) OVER (PARTITION BY merchant ORDER BY txn_time, id) + INTERVAL '30' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM filtered
),
sessions_transactions AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY merchant ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM with_gaps_2
),
with_gaps_3 AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN txn_time - LAG(txn_time) OVER (PARTITION BY merchant ORDER BY txn_time, id) > INTERVAL '90' DAY THEN 1 ELSE 0 END AS is_new_burst_2
    FROM sessions_transactions
),
sessions_transactions_2 AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst_2) OVER (PARTITION BY merchant ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM with_gaps_3
),
matched_keys AS (
    -- Set intersection on keys
    SELECT merchant
    FROM sessions_transactions_2
    INTERSECT ALL
    SELECT merchant
    FROM transactions
    WHERE txn_type = 'CREDIT'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM sessions_transactions_2 s
    INNER JOIN matched_keys k ON s.merchant = k.merchant
)
-- Multi-level aggregation
SELECT
    CASE WHEN is_new_burst_2 >= 1 THEN 'PREMIUM' ELSE 'STANDARD' END AS is_new_burst_2_tier,
    island_id,
    MIN(is_new_burst_2) AS min_is_new_burst_2,
    COUNT(*) FILTER (WHERE merchant = 'Shell') AS shell_rows
FROM kept_rows
GROUP BY GROUPING SETS ((CASE WHEN is_new_burst_2 >= 1 THEN 'PREMIUM' ELSE 'STANDARD' END, island_id), (CASE WHEN is_new_burst_2 >= 1 THEN 'PREMIUM' ELSE 'STANDARD' END));
