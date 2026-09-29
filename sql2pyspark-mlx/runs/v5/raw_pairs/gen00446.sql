SELECT product_id, SUM(quantity) AS total_quantity, ROUND(STDDEV(quantity), 2) AS stddev_quantity
FROM inventory
WHERE store_id = 1
GROUP BY product_id;
