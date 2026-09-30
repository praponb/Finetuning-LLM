SELECT *, LOWER(brand) AS brand_lower, ROUND(price, 1) AS price_rounded
FROM products
WHERE ARRAY_CONTAINS(tags, 'eco')
