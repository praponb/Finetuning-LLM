SELECT store_id, ROUND(AVG(quantity), 2) AS avg_quantity, MAX(last_restock) AS last_last_restock
FROM inventory
GROUP BY store_id
HAVING ROUND(AVG(quantity), 2) < 62
ORDER BY 3 ASC, store_id ASC
LIMIT 20
