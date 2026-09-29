SELECT order_id, SUM(unit_price) total_unit_price FROM order_items GROUP BY order_id HAVING SUM(unit_price) >= 2100 ORDER BY order_id LIMIT 20;
