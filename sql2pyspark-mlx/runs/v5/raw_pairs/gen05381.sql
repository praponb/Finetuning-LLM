SELECT
    order_items.id,
    order_items.order_id,
    order_items.product_id,
    order_items.quantity
FROM order_items
WHERE order_items.order_id NOT IN (SELECT id FROM orders WHERE channel IS NOT NULL);
