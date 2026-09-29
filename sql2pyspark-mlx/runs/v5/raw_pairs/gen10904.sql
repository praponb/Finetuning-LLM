WITH kept_keys AS (
    SELECT merchant, id
    FROM transactions
    INTERSECT ALL
    SELECT merchant, id
    FROM transactions
    WHERE txn_type IN ('REFUND', 'FEE', 'CREDIT')
),
verified AS (
    SELECT s.*
    FROM transactions s
    JOIN kept_keys k ON s.merchant = k.merchant AND s.id = k.id
)
SELECT
    CASE WHEN amount >= 3300 THEN 'HIGH' ELSE 'LOW' END AS amount_tier,
    MAX(amount) FILTER (WHERE merchant = 'Amazon') AS amazon_max_amount,
    STDDEV_SAMP(amount) AS amount_stddev,
    MIN(txn_time) AS first_txn_time,
    ROUND(SUM(amount) / NULLIF(COUNT(CASE WHEN merchant = 'Amazon' THEN 1 END), 0), 2) AS amount_per_amazon,
    MAX(txn_time) AS ended_at,
    COUNT(*) FILTER (WHERE merchant = 'Starbucks') AS starbucks_rows
FROM verified
GROUP BY CASE WHEN amount >= 3300 THEN 'HIGH' ELSE 'LOW' END
ORDER BY amazon_max_amount DESC;
