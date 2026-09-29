SELECT CASE WHEN cost >= 104 THEN 'high' WHEN cost >= 43 THEN 'mid' ELSE 'low' END AS cost_bucket, MAX(shipped_date) AS latest_shipped_date
FROM shipments
GROUP BY cost_bucket
ORDER BY cost_bucket ASC, latest_shipped_date ASC;
