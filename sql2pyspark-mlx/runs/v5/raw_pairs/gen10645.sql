SELECT order_id, AVG(unit_price) AS avg_unit_price, MAX(unit_price) - MIN(unit_price) AS unit_price_range FROM order_items GROUP BY order_id
