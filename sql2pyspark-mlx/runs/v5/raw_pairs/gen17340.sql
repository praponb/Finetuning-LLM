SELECT store_id, stddev_quantity FROM (SELECT store_id, ROUND(STDDEV(quantity), 2) stddev_quantity FROM inventory GROUP BY store_id) t WHERE stddev_quantity >= 27;
