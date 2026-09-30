SELECT order_id, AVG(unit_price) AS avg_unit_price FROM order_items GROUP BY order_id HAVING SUM(quantity) > 19 ORDER BY avg_unit_price DESC
