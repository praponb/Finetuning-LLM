SELECT id, quantity, quantity * unit_price AS line_total
FROM order_items
WHERE NOT (order_id = 28 AND order_id = 23 AND order_id IN (12, 13, 16))
ORDER BY line_total DESC;
