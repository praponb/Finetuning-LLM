SELECT * FROM (SELECT product_id, COUNT(quantity) quantity_count, ROUND(STDDEV(quantity), 2) stddev_quantity FROM inventory GROUP BY product_id) x WHERE stddev_quantity < 42
