SELECT order_items.id, order_items.unit_price, order_items.order_id
FROM order_items
WHERE order_items.order_id IN (SELECT id FROM orders WHERE channel IN ('store', 'partner'))
