SELECT shipping_fee
FROM orders
INTERSECT
SELECT price
FROM products
WHERE brand IN ('Globex', 'Stark')
ORDER BY shipping_fee;
