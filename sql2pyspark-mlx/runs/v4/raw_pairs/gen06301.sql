WITH valid AS (
    SELECT
        id,
        product_id,
        sale_date,
        units,
        revenue,
        ROUND(units * 1.1, 2) AS units_scaled
    FROM store_sales
    WHERE store_id IN (2, 4)
)
SELECT *
FROM (
    SELECT
        id,
        product_id,
        units_scaled,
        revenue
    FROM valid
) src
UNPIVOT (
    reading FOR metric_name IN (units_scaled, revenue)
)
