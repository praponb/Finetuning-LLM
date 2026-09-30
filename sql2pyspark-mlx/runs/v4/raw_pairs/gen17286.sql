WITH base AS (
    SELECT
        id,
        product_name,
        brand,
        price,
        cost,
        stock_qty,
        tags,
        supplier_id,
        price / NULLIF(stock_qty, 0) price_per_stock_qty,
        COALESCE(brand, 'NONE') brand_clean
    FROM products
    WHERE launch_date >= TO_DATE('2023-10-13')
      AND brand IS NOT NULL
      AND brand IN ('Globex', 'Umbrella')
)
SELECT *
FROM (
    SELECT
        supplier_id,
        brand,
        price_per_stock_qty
    FROM base
) src
PIVOT (
    MAX(price_per_stock_qty) AS value_sum
    FOR brand IN ('Stark' AS stark, 'Globex' AS globex)
)
