SELECT *
FROM (
    SELECT
        supplier_id,
        category,
        cost
    FROM products
) src
PIVOT (
    MAX(cost) AS total,
    COUNT(1) AS n
    FOR category IN ('Books', 'Home', 'Fashion')
)
