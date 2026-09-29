SELECT product_id, MIN(quantity) min_quantity, MAX(last_restock) last_last_restock
FROM inventory
GROUP BY product_id
HAVING MIN(quantity) >= 200
ORDER BY product_id DESC
