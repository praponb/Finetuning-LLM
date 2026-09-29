SELECT
    id,
    brand,
    price,
    ROW_NUMBER() OVER (PARTITION BY brand ORDER BY price DESC, id) AS price_row
FROM products
