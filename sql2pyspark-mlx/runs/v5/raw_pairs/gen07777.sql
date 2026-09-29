SELECT order_items.id, order_items.unit_price, order_items.quantity
FROM order_items
WHERE order_items.product_id IN (SELECT id FROM products WHERE cost < 690)
ORDER BY order_items.quantity;
