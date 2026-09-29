SELECT * FROM (SELECT order_id, COUNT(DISTINCT quantity) unique_products, COUNT(quantity) quantity_count FROM order_items WHERE quantity <> 6 GROUP BY order_id) t WHERE unique_products >= 9
