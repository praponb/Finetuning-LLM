SELECT store_id, ROUND(STDDEV(quantity), 2) AS stddev_quantity FROM inventory WHERE store_id = 4 AND quantity <= 159 GROUP BY store_id;
