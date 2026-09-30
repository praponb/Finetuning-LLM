SELECT quantity, product_id
FROM order_items
WHERE quantity BETWEEN 5 AND 6
UNION
SELECT quantity, product_id
FROM order_items
WHERE unit_price <= 240
ORDER BY quantity DESC
