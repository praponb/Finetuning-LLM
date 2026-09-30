SELECT order_id, quantity, unit_price
FROM order_items
WHERE product_id IS NOT NULL;
