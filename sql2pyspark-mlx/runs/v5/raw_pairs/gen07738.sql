WITH base AS (
    -- Keep recent, valid rows
    SELECT *, COALESCE(cost, 0) AS cost_filled
    FROM products
    WHERE launch_date >= TO_DATE('2023-06-12')
      AND brand IS NOT NULL
)
-- Native SQL UNPIVOT (wide to long)
SELECT
    id,
    category,
    measure,
    value
FROM (
    SELECT
        id,
        category,
        price,
        cost_filled,
        cost
    FROM base
) src
UNPIVOT INCLUDE NULLS (
    value FOR measure IN (price AS PRICE, cost_filled AS COST_FILLED, cost AS COST)
);
