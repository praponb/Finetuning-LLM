WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_id,
        product_id,
        units,
        revenue,
        units * revenue AS revenue_per_unit
    FROM store_sales
    WHERE sale_date >= DATE '2023-04-16'
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id, product_id ORDER BY revenue_per_unit DESC NULLS LAST, id) AS rn
    FROM cleaned
    QUALIFY rn = 1
),
with_dims AS (
    -- Attach products attributes
    SELECT
        src.*,
        p.launch_date,
        p.stock_qty
    FROM deduped src
    LEFT JOIN products p
        ON src.product_id = p.id
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY stock_qty DESC, id) AS latest_rank
        FROM with_dims
    ) x
    WHERE latest_rank = 1
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        product_id,
        units,
        revenue,
        revenue_per_unit,
        stock_qty,
        latest_rank,
        units / NULLIF(revenue_per_unit, 0) AS units_per_revenue_per_unit,
        DAYOFWEEK(launch_date) AS launch_date_dayofweek
    FROM latest
    WHERE rn = 1
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        PERCENT_RANK() OVER w_part AS revenue_percent_rank,
        NTILE(4) OVER w_ordered AS launch_date_dayofweek_bucket
    FROM valid
    WINDOW w_part AS (PARTITION BY product_id ORDER BY revenue NULLS FIRST), w_ordered AS (PARTITION BY product_id ORDER BY launch_date_dayofweek DESC, id)
),
enriched AS (
    -- Attach products attributes
    SELECT src.*, p.cost
    FROM with_windows src
    LEFT JOIN products p
        ON src.product_id = p.id
)
-- Aggregate with conditional metrics
SELECT
    product_id,
    CASE WHEN revenue_percent_rank >= 0.61 THEN 'PREMIUM' ELSE 'STANDARD' END AS revenue_percent_rank_tier,
    CASE WHEN units_per_revenue_per_unit >= 73 THEN 'PREMIUM' ELSE 'STANDARD' END AS units_per_revenue_per_unit_tier,
    SUM(units) AS total_units,
    PERCENTILE_APPROX(launch_date_dayofweek_bucket, ARRAY(0.25, 0.5, 0.75)) AS launch_date_dayofweek_bucket_quartiles,
    GROUPING_ID() AS grouping_id
FROM enriched
GROUP BY CUBE (product_id, CASE WHEN revenue_percent_rank >= 0.61 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN units_per_revenue_per_unit >= 73 THEN 'PREMIUM' ELSE 'STANDARD' END)
ORDER BY product_id DESC
