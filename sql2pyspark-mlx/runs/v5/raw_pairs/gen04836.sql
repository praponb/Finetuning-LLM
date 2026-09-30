SELECT CASE WHEN quantity >= 240 THEN 'high' WHEN quantity >= 75 THEN 'mid' ELSE 'low' END AS quantity_bucket, COUNT(quantity) AS quantity_count FROM inventory GROUP BY quantity_bucket;
