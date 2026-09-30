WITH recent_shipments AS (
    SELECT *, CASE WHEN cost >= 91 THEN 'High' WHEN cost >= 33 THEN 'Medium' ELSE 'Low' END AS cost_band
    FROM shipments
    WHERE status != 'RETURNED' OR cost BETWEEN 72 AND 86
),
summary AS (
    SELECT CASE WHEN cost >= 87 THEN 'high' WHEN cost >= 34 THEN 'mid' ELSE 'low' END AS cost_bucket, DATE_TRUNC('MONTH', delivered_date) AS delivered_month, ROUND(AVG(cost), 2) AS avg_cost
    FROM recent_shipments
    GROUP BY CASE WHEN cost >= 87 THEN 'high' WHEN cost >= 34 THEN 'mid' ELSE 'low' END, DATE_TRUNC('MONTH', delivered_date)
)
SELECT cost_bucket, delivered_month
FROM summary
WHERE avg_cost > 74
ORDER BY delivered_month DESC;
