SELECT order_id
FROM order_items
WHERE quantity > 8
UNION
SELECT order_id
FROM order_items
WHERE product_id = 10
