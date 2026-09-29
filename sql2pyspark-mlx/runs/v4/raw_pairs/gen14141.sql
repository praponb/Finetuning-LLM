SELECT id, category, cost
FROM (
    SELECT
        id,
        category,
        cost,
        ROW_NUMBER() OVER (PARTITION BY category ORDER BY cost DESC, id) AS row_num
    FROM products
    WHERE SIZE(tags) > 2
) AS r
WHERE row_num <= 2;
