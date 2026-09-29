SELECT product_id, ROUND(STDDEV(quantity), 2) AS stddev_quantity, AVG(quantity) AS avg_quantity
FROM inventory
WHERE product_id = 1 AND product_id IN (1, 5)
GROUP BY product_id
ORDER BY avg_quantity, stddev_quantity DESC, product_id
LIMIT 10
