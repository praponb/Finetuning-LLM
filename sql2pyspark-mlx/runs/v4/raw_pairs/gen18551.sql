WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        txn_type,
        amount,
        txn_time,
        merchant,
        LENGTH(merchant) AS merchant_length,
        DATEDIFF(CURRENT_DATE(), txn_time) AS days_since_txn_time
    FROM transactions
    WHERE txn_time > TIMESTAMP '2023-10-22 14:00:00'
      AND merchant IS NOT NULL
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN txn_time - LAG(txn_time) OVER (PARTITION BY txn_type ORDER BY txn_time, id) > INTERVAL 90 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM valid
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY txn_type ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM with_gaps
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM sessionized
    QUALIFY ROW_NUMBER() OVER (PARTITION BY txn_type, merchant ORDER BY days_since_txn_time, id) = 1
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        txn_type,
        merchant,
        merchant_length
    FROM ranked
) src
PIVOT (
    SUM(merchant_length) AS total,
    COUNT(1) AS item_count
    FOR merchant IN ('7-Eleven' AS v_7_eleven, 'Starbucks' AS starbucks, 'Lazada' AS lazada)
);
