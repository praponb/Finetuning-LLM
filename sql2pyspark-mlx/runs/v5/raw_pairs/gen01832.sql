WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        store_id,
        sale_date,
        units,
        revenue - units AS revenue_per_unit
    FROM store_sales
    WHERE sale_date >= TO_DATE('2023-05-08')
)
-- Keep one row per key (latest first)
SELECT
    id,
    store_id,
    sale_date,
    units,
    revenue_per_unit
FROM cleaned
QUALIFY RANK() OVER (PARTITION BY store_id ORDER BY sale_date ASC) = 1
