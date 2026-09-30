SELECT
    id,
    category,
    price,
    ROUND(price / SUM(price) OVER (PARTITION BY category), 3) AS price_share
FROM products
ORDER BY price DESC
