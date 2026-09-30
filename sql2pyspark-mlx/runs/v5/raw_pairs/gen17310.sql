SELECT order_id, id
FROM order_items
EXCEPT
SELECT order_id, id
FROM order_items
WHERE quantity <> 5
