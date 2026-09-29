SELECT CASE WHEN cost >= 78 THEN 'high' WHEN cost >= 32 THEN 'mid' ELSE 'low' END AS cost_bucket, AVG(cost) AS avg_cost
FROM shipments
WHERE status = 'IN_TRANSIT'
GROUP BY 1;
