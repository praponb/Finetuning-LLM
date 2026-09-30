SELECT order_items.id, order_items.product_id, order_items.order_id
FROM order_items
WHERE order_items.quantity BETWEEN 6 AND 8 AND order_items.product_id IN (SELECT id FROM products WHERE product_name LIKE 'N%')
