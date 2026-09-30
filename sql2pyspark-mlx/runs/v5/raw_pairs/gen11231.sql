SELECT id, quantity, unit_price - quantity AS line_total FROM order_items ORDER BY 3 DESC;
