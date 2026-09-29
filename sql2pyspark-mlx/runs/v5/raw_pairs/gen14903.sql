WITH valid AS (
    SELECT
        *,
        stock_qty - price AS stock_qty_minus_price,
        SUBSTRING(brand, 1, 2) AS brand_prefix
    FROM products
    WHERE launch_date >= DATE '2023-06-24'
      AND supplier_id IS NOT NULL
)
SELECT *
FROM (
    SELECT
        supplier_id,
        category,
        price
    FROM valid
) src
PIVOT (
    SUM(price) AS value_sum,
    COUNT(1) AS cnt
    FOR category IN ('Electronics', 'Grocery')
);
