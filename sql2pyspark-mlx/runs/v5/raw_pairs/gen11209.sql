SELECT * FROM (SELECT order_id, product_id, COUNT(*) total_count, AVG(quantity) avg_quantity FROM order_items GROUP BY order_id, product_id) sub WHERE avg_quantity > 3.7;
