SELECT CASE WHEN cost >= 99 THEN 'high' WHEN cost >= 49 THEN 'mid' ELSE 'low' END AS cost_bucket, status, ROUND(AVG(cost), 2) AS avg_cost
FROM shipments
GROUP BY CASE WHEN cost >= 99 THEN 'high' WHEN cost >= 49 THEN 'mid' ELSE 'low' END, status
