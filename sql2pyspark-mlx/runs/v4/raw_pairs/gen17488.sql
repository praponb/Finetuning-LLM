SELECT id, order_id, product_id
FROM order_items
WHERE product_id IS NOT NULL;
