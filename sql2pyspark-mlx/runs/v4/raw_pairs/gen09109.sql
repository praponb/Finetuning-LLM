SELECT id, order_id, quantity, ROUND(unit_price * 1.15, 2) AS adjusted_unit_price FROM order_items ORDER BY order_id DESC
