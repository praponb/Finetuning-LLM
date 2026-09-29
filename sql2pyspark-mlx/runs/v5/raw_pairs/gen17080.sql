SELECT shipping_fee
FROM orders
UNION
SELECT cost
FROM products
ORDER BY shipping_fee
