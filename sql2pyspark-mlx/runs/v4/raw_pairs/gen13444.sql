SELECT product_id
FROM order_items
WHERE product_id IN (4, 8)
EXCEPT
SELECT product_id
FROM order_items
WHERE order_id = 17;
