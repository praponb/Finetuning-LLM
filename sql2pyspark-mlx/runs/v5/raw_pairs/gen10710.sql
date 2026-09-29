SELECT product_id, store_id, AVG(quantity) AS avg_quantity
FROM inventory
GROUP BY product_id, store_id
ORDER BY avg_quantity
