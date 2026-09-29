SELECT order_id
FROM order_items
EXCEPT
SELECT order_id
FROM order_items
WHERE quantity = 4
