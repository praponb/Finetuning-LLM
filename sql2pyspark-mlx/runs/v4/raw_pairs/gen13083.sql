SELECT product_id, ROUND(STDDEV(quantity), 2) AS stddev_quantity
FROM inventory
GROUP BY product_id
HAVING ROUND(STDDEV(quantity), 2) < 36
ORDER BY product_id DESC, stddev_quantity
LIMIT 3
