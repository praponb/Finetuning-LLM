SELECT id, category, price
FROM (
    SELECT
        id,
        category,
        price,
        ROW_NUMBER() OVER (PARTITION BY category ORDER BY price DESC, id) AS rnk
    FROM products
) AS ranked
WHERE rnk = 1
ORDER BY category;
