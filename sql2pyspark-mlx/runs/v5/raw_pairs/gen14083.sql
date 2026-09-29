WITH matched_keys AS (
    SELECT merchant, id
    FROM transactions
    INTERSECT
    SELECT merchant, id
    FROM transactions
    WHERE HOUR(txn_time) BETWEEN 8 AND 13
),
reconciled AS (
    SELECT s.*
    FROM transactions s
    INNER JOIN matched_keys k ON s.merchant = k.merchant AND s.id = k.id
)
SELECT
    CASE WHEN amount >= 3900 THEN 'HIGH' ELSE 'LOW' END AS amount_tier,
    MAX(txn_time) AS last_txn_time,
    MIN(txn_time) AS started_at,
    ROUND(AVG(amount), 2) AS avg_amount,
    MAX(amount) FILTER (WHERE merchant = 'Starbucks') AS starbucks_max_amount,
    COUNT_IF(txn_type = 'FEE') AS fee_events,
    ROUND(SUM(amount) / NULLIF(COUNT(CASE WHEN merchant = 'Starbucks' THEN 1 END), 0), 2) AS amount_per_starbucks
FROM reconciled
GROUP BY CASE WHEN amount >= 3900 THEN 'HIGH' ELSE 'LOW' END
ORDER BY amount_per_starbucks DESC NULLS LAST
