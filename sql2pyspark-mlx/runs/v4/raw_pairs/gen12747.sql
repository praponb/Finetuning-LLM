WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        GREATEST(price, cost) AS greatest_price_cost,
        ROUND(price * 100, 2) AS adjusted_price,
        COALESCE(brand, 'OTHER') AS brand_clean
    FROM products
    WHERE launch_date >= DATE '2023-06-28'
      AND brand IS NOT NULL
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        id,
        category,
        cost,
        greatest_price_cost
    FROM base
) src
UNPIVOT (
    reading FOR metric_name IN (cost AS COST_VAL, greatest_price_cost AS GREATEST_PRICE_COST)
)
