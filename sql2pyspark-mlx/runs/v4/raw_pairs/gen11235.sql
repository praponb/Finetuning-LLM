SELECT product_id
FROM order_items
INTERSECT
SELECT product_id
FROM order_items
WHERE unit_price >= 380
