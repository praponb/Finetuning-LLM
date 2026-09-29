WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue,
        ROUND(units / revenue, 2) AS revenue_per_unit
    FROM store_sales
    WHERE sale_date > '2025-03-16'
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        id,
        product_id,
        revenue_per_unit,
        revenue
    FROM filtered
) src
UNPIVOT (
    val FOR metric_name IN (revenue_per_unit, revenue)
)
