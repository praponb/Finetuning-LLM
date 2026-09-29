SELECT store_id, ROUND(STDDEV(quantity), 2) stddev_quantity FROM inventory WHERE store_id IN (3, 5) GROUP BY store_id
