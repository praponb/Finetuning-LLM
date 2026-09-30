SELECT id, product_id
FROM order_items
EXCEPT
SELECT id, product_id
FROM order_items
WHERE quantity >= 5
