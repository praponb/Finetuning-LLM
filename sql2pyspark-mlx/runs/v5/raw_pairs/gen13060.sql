WITH base AS (
    -- Basic cleansing and defaults
    SELECT *, GREATEST(revenue, units) AS greatest_revenue_units
    FROM store_sales
    WHERE sale_date > DATE '2023-09-02'
),
enriched AS (
    -- Attach stores attributes
    SELECT
        s.id,
        s.store_id,
        s.product_id,
        s.units,
        b.store_name
    FROM base s
    LEFT JOIN stores b
        ON s.store_id = b.id
    WHERE LENGTH(b.store_name) < 6
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        product_id,
        units,
        store_name,
        ROUND(units, 1) AS units_rounded,
        CAST(units AS DOUBLE) AS units_num
    FROM enriched
    WHERE units < 29
),
joined AS (
    -- Attach products attributes
    SELECT
        s.id,
        s.product_id,
        s.units,
        s.store_name,
        s.units_rounded,
        s.units_num,
        d.stock_qty,
        d.brand
    FROM valid s
    JOIN products d
        ON s.product_id = d.id
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        product_id,
        units,
        units_rounded,
        units_num
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY brand ORDER BY stock_qty NULLS LAST, id) AS row_num
        FROM joined
    ) x
    WHERE row_num = 1
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        product_id,
        units,
        units_rounded,
        CAST(units AS DOUBLE) AS units_str,
        ROUND(units * 0.9, 2) AS units_adjusted
    FROM deduped
    WHERE units_num > 42
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN units_rounded >= 83 THEN 'HIGH' ELSE 'LOW' END AS units_rounded_tier,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(DISTINCT id) AS distinct_ids_2,
    APPROX_PERCENTILE(units_adjusted, 0.95) AS units_adjusted_p95,
    GROUPING_ID() AS gid
FROM cleaned
GROUP BY ROLLUP (CASE WHEN units_rounded >= 83 THEN 'HIGH' ELSE 'LOW' END)
HAVING APPROX_PERCENTILE(units_adjusted, 0.95) > 0
ORDER BY distinct_ids_2 DESC NULLS FIRST
