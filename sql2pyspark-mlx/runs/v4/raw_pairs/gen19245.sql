SELECT order_id, quantity
FROM order_items
WHERE product_id IS NULL
UNION ALL
SELECT order_id, quantity
FROM order_items
WHERE unit_price > 200;
