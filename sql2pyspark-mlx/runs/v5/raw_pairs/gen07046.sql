WITH base AS (
    SELECT
        *,
        UPPER(category) AS category_upper,
        COALESCE(brand, 'Unknown') AS brand_filled
    FROM products
    WHERE launch_date >= DATE '2023-05-23'
)
SELECT *
FROM (
    SELECT
        id,
        brand,
        cost,
        price
    FROM base
) src
UNPIVOT INCLUDE NULLS (
    reading FOR kpi IN (cost, price)
);
