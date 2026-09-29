SELECT ROUND(AVG(quantity), 2) AS avg_quantity FROM order_items WHERE product_id = 8 AND order_id = 38 ORDER BY avg_quantity
