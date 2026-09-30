SELECT order_id, product_id, ROUND(STDDEV(unit_price), 2) AS stddev_unit_price FROM order_items WHERE product_id = 29 GROUP BY order_id, product_id ORDER BY product_id DESC, 1 DESC
