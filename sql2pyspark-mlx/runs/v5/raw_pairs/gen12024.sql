WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT *, DATE_FORMAT(txn_time, 'yyyy-MM') AS txn_time_label
    FROM transactions
    WHERE txn_time > TO_DATE('2023-07-06')
)
-- Grouped metrics
SELECT
    DATE_TRUNC('QUARTER', txn_time) AS quarter_start,
    CASE WHEN amount >= 4200 THEN 'HIGH' ELSE 'LOW' END AS amount_tier,
    ROUND(SUM(amount) / NULLIF(COUNT(CASE WHEN merchant = '7-Eleven' THEN 1 END), 0), 2) AS amount_per_7_eleven,
    (UNIX_TIMESTAMP(MAX(txn_time)) - UNIX_TIMESTAMP(MIN(txn_time))) / 60 AS duration_min
FROM cleaned
GROUP BY 1, 2;
