SELECT CASE WHEN cost >= 80 THEN 'high' WHEN cost >= 30 THEN 'mid' ELSE 'low' END AS cost_bucket, ROUND(AVG(cost), 2) AS avg_cost
FROM shipments
WHERE carrier IN ('FedEx', 'DHL', 'Flash') AND cost > 46 AND cost BETWEEN 66 AND 85
GROUP BY CASE WHEN cost >= 80 THEN 'high' WHEN cost >= 30 THEN 'mid' ELSE 'low' END
