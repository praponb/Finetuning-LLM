SELECT id, product_name, CAST(price AS INT) AS price_int
FROM products
WHERE NOT (cost < 500 AND ARRAY_CONTAINS(tags, 'premium'));
