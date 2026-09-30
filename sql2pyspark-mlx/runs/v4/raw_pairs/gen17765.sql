SELECT id, unit_price, unit_price - quantity AS line_total
FROM order_items
WHERE order_id IN (21, 37);
