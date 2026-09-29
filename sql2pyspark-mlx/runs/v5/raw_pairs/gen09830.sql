SELECT
    id,
    category,
    brand,
    price
FROM products
WHERE SIZE(tags) >= 2 OR price >= 750;
