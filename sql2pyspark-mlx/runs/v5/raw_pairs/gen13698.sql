WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        UPPER(product_name) AS product_name_upper,
        GREATEST(cost, price) AS greatest_cost_price
    FROM products
    WHERE supplier_id IS NOT NULL
      AND YEAR(launch_date) >= 2023
)
-- Join a dimension and aggregate
SELECT
    s.country,
    s.city,
    a.brand,
    COUNT(*) AS cnt,
    ROUND(SUM(a.cost), 2) AS total_cost,
    MIN(a.stock_qty) AS min_stock_qty,
    GROUPING_ID() AS grouping_level
FROM valid a
LEFT JOIN suppliers s ON a.supplier_id = s.id
GROUP BY ROLLUP (s.country, s.city, a.brand)
ORDER BY brand ASC;
