SELECT price, 'product' AS source
FROM products
WHERE ARRAY_CONTAINS(tags, 'eco')
UNION
SELECT shipping_fee, 'order' AS source
FROM orders
ORDER BY price;
