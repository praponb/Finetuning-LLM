WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        stock_qty + price AS stock_qty_plus_price,
        COALESCE(brand, 'N/A') AS brand_clean
    FROM products
    WHERE brand IS DISTINCT FROM 'Umbrella'
      AND price IS NULL
)
-- Native SQL UNPIVOT (wide to long)
SELECT
    id,
    brand,
    metric,
    reading
FROM (
    SELECT
        id,
        brand,
        cost,
        price,
        stock_qty_plus_price
    FROM cleaned
) src
UNPIVOT INCLUDE NULLS (
    reading FOR metric IN (cost, price, stock_qty_plus_price)
);
