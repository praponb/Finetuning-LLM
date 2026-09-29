SELECT
    id,
    category,
    price,
    ROW_NUMBER() OVER (PARTITION BY category ORDER BY price, id) AS price_row
FROM products
ORDER BY price DESC
