SELECT CASE WHEN quantity >= 199 THEN 'high' WHEN quantity >= 61 THEN 'mid' ELSE 'low' END AS quantity_bucket, MIN(last_restock) AS first_last_restock
FROM inventory
GROUP BY CASE WHEN quantity >= 199 THEN 'high' WHEN quantity >= 61 THEN 'mid' ELSE 'low' END
ORDER BY quantity_bucket DESC, first_last_restock;
