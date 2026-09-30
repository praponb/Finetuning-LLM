SELECT CASE WHEN cost >= 83 THEN 'high' WHEN cost >= 25 THEN 'mid' ELSE 'low' END AS cost_bucket, COUNT(cost) AS cost_count, COUNT(*) AS num_shipments
FROM shipments
GROUP BY cost_bucket
ORDER BY cost_bucket DESC;
