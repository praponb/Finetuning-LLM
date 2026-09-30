WITH top_products AS (
    SELECT
        id,
        category,
        price,
        ROW_NUMBER() OVER (PARTITION BY category ORDER BY price, id) AS rnk
    FROM products
)
SELECT id, category, price
FROM top_products
WHERE rnk <= 2
ORDER BY category ASC
