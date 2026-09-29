WITH with_dims AS (
    -- Attach products attributes
    SELECT
        e.id,
        e.product_id,
        e.quantity,
        b.category,
        b.launch_date
    FROM inventory e
    LEFT JOIN products b
        ON e.product_id = b.id
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(quantity) OVER (PARTITION BY product_id ORDER BY launch_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) last7_avg_quantity,
        COUNT(quantity) OVER (PARTITION BY product_id ORDER BY launch_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) last7_count_quantity,
        MIN(quantity) OVER (PARTITION BY product_id ORDER BY launch_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) last7_min_quantity
    FROM with_dims
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity NULLS FIRST, id) AS rank_in_group
    FROM scored
    QUALIFY rank_in_group = 1
),
with_dims_inventory AS (
    -- Attach products attributes
    SELECT
        a.id,
        a.product_id,
        a.category,
        a.last7_avg_quantity,
        a.last7_count_quantity,
        a.rank_in_group,
        d.product_name,
        d.launch_date AS product_launch_date,
        d.cost
    FROM deduped a
    LEFT JOIN products d
        ON a.product_id = d.id
    WHERE d.cost > 340
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        cost / NULLIF(last7_avg_quantity, 0) cost_per_last7_avg_quantity,
        ROUND(last7_count_quantity * 1.15, 2) adjusted_last7_count_quantity
    FROM with_dims_inventory
    WHERE cost BETWEEN 350 AND 710
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN last7_avg_quantity >= 218 THEN 'LARGE' ELSE 'SMALL' END AS last7_avg_quantity_tier,
    DATE_TRUNC('YEAR', product_launch_date) AS year_start,
    APPROX_PERCENTILE(last7_count_quantity, ARRAY(0.25, 0.5, 0.75)) AS last7_count_quantity_quartiles,
    AVG(cost_per_last7_avg_quantity) FILTER (WHERE category = 'Books') AS books_avg_cost_per_last7_avg_quantity
FROM valid
GROUP BY CUBE (CASE WHEN last7_avg_quantity >= 218 THEN 'LARGE' ELSE 'SMALL' END, DATE_TRUNC('YEAR', product_launch_date))
