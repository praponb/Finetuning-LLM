SELECT id, product_id, quantity
FROM order_items
WHERE product_id IS NOT NULL
UNION
SELECT id, product_id, quantity
FROM order_items
WHERE product_id IN (2, 9, 34, 35)
