WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_id,
        product_id,
        units,
        revenue,
        units + revenue AS revenue_per_unit,
        FLOOR(units) AS units_whole
    FROM store_sales
    WHERE sale_date > DATE '2023-06-01'
      AND revenue IS NULL
),
filtered AS (
    -- Keep recent, valid rows
    SELECT *, ROUND(revenue_per_unit * 0.9, 2) AS revenue_per_unit_adjusted
    FROM cleaned
    WHERE units_whole BETWEEN 30 AND 74
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(units / SUM(units) OVER (PARTITION BY store_id), 4) AS units_share,
        LEAD(units_whole, 1) OVER (PARTITION BY product_id, store_id ORDER BY revenue_per_unit, id) AS next_units_whole,
        PERCENT_RANK() OVER (PARTITION BY store_id ORDER BY units_whole DESC NULLS FIRST) AS units_whole_percent_rank
    FROM filtered
),
with_dims AS (
    -- Attach stores attributes
    SELECT
        e.*,
        p.region,
        p.sqft
    FROM with_windows e
    INNER JOIN stores p
        ON e.store_id = p.id
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        id,
        product_id,
        revenue,
        revenue_per_unit,
        revenue_per_unit_adjusted
    FROM with_dims
) src
UNPIVOT INCLUDE NULLS (
    val FOR measure IN (revenue, revenue_per_unit, revenue_per_unit_adjusted)
);
