SELECT product_id, ROUND(STDDEV(quantity), 2) AS stddev_quantity, SUM(quantity) AS total_quantity FROM inventory GROUP BY product_id
