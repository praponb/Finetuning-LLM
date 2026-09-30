SELECT product_id, SUM(quantity) total_quantity
FROM inventory
WHERE store_id = 4
GROUP BY product_id
HAVING SUM(quantity) >= 1210
