SELECT * FROM (SELECT product_id, order_id, ROUND(STDDEV(unit_price), 2) AS stddev_unit_price FROM order_items GROUP BY product_id, order_id) AS t WHERE stddev_unit_price >= 64;
