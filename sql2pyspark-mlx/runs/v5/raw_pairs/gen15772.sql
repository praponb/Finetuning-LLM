SELECT CASE WHEN quantity >= 233 THEN 'high' WHEN quantity >= 118 THEN 'mid' ELSE 'low' END AS quantity_bucket, MAX(last_restock) AS last_last_restock
FROM inventory
WHERE NOT (store_id IN (1, 6) AND store_id IN (2, 3, 4, 6) AND last_restock >= '2023-06-11')
GROUP BY quantity_bucket
ORDER BY quantity_bucket
