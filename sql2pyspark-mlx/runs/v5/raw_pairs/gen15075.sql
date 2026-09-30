WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CASE WHEN units >= 45 THEN 'High' WHEN units >= 8 THEN 'Medium' ELSE 'Low' END units_band,
        ROUND(units / revenue, 2) revenue_per_unit
    FROM store_sales
    WHERE sale_date >= DATE '2023-10-06'
)
-- Join a dimension and aggregate
SELECT
    d.brand,
    COUNT(*) AS cnt,
    MIN(e.units) AS min_units
FROM filtered e
INNER JOIN products d ON e.product_id = d.id
GROUP BY d.brand;
