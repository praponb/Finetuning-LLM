SELECT store_id, COUNT(DISTINCT quantity) AS unique_stores, MAX(quantity) AS max_quantity
FROM inventory
GROUP BY store_id
HAVING MAX(quantity) > 230
ORDER BY store_id DESC
