WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        store_id,
        units,
        revenue,
        ROUND(units, 0) AS units_rounded,
        units * revenue AS revenue_per_unit
    FROM store_sales
    WHERE sale_date >= TO_DATE('2024-02-07')
      AND product_id IN (1, 2, 10)
)
-- Running and moving aggregates
SELECT
    id,
    store_id,
    units,
    revenue,
    units_rounded,
    revenue_per_unit,
    SUM(units_rounded) OVER (PARTITION BY store_id) AS store_id_sum_units_rounded,
    CUME_DIST() OVER (ORDER BY units DESC) AS units_cume_dist
FROM filtered
