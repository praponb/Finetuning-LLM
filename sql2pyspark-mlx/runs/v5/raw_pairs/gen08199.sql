SELECT id, product_id, order_id
FROM order_items
WHERE order_id IN (6, 16, 19, 35)
UNION ALL
SELECT id, product_id, order_id
FROM order_items
WHERE unit_price IS NOT NULL
ORDER BY order_id;
