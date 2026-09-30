SELECT order_id, MAX(unit_price) AS max_unit_price, COUNT(DISTINCT quantity) AS unique_orders FROM order_items WHERE quantity > 6 GROUP BY order_id;
