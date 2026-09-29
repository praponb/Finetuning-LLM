SELECT
    id,
    brand,
    price,
    product_name
FROM (
    SELECT
        id,
        brand,
        price,
        product_name,
        ROW_NUMBER() OVER (PARTITION BY brand ORDER BY price DESC, id) AS rnk
    FROM products
) AS r
WHERE rnk <= 3;
