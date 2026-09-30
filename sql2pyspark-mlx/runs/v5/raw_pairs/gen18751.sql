SELECT id, order_id
FROM order_items
WHERE quantity = 7
EXCEPT
SELECT id, order_id
FROM order_items
WHERE order_id IN (2, 4, 19)
ORDER BY order_id
