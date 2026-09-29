WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_id,
        units,
        revenue,
        revenue - units AS revenue_per_unit
    FROM store_sales
    WHERE sale_date > TO_DATE('2024-03-01')
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        id,
        store_id,
        revenue_per_unit,
        revenue
    FROM valid
) src
UNPIVOT (
    metric_value FOR metric IN (revenue_per_unit, revenue)
)
