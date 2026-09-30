SELECT
    CASE WHEN cost >= 86 THEN 'high' WHEN cost >= 19 THEN 'mid' ELSE 'low' END AS cost_bucket,
    MAX(delivered_date) AS last_delivered_date,
    MIN(shipped_date) AS first_shipped_date,
    MAX(cost) AS max_cost
FROM shipments
WHERE cost <= 59 AND status IN ('IN_TRANSIT', 'LOST', 'DELIVERED')
GROUP BY cost_bucket
HAVING max_cost >= 80
ORDER BY first_shipped_date, last_delivered_date
