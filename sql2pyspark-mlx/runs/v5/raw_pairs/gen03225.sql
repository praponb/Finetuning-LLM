SELECT product_id, id
FROM order_items
INTERSECT ALL
SELECT product_id, id
FROM order_items
WHERE unit_price < 260
