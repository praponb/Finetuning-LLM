SELECT unit_price AS value
FROM order_items
WHERE order_id IN (9, 18, 19, 26)
INTERSECT
SELECT price AS value
FROM products
WHERE brand = 'Stark'
ORDER BY value;
