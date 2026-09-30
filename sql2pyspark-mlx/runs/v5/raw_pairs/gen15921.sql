SELECT order_id, MAX(quantity) AS max_quantity, ROUND(STDDEV(quantity), 2) AS stddev_quantity FROM order_items WHERE product_id = 36 AND product_id = 24 GROUP BY 1 HAVING MAX(quantity) > 7.7;
