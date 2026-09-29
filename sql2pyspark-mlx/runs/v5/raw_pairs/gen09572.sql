WITH ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM store_sales
    QUALIFY ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY revenue DESC NULLS LAST, id) = 1
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        revenue + units AS revenue_per_unit,
        CASE WHEN revenue >= 3600 THEN 'High' WHEN revenue >= 1600 THEN 'Medium' ELSE 'Low' END AS revenue_tier
    FROM ranked
    WHERE revenue >= 1400
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, DATEDIFF(CURRENT_DATE(), sale_date) AS days_since_sale_date
    FROM filtered
    WHERE units = 18
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        store_id,
        product_id,
        units,
        revenue,
        revenue_per_unit,
        revenue_tier,
        COUNT(units) OVER w_recent AS last7_count_units,
        SUM(units) OVER w_recent AS last7_sum_units,
        MAX(units) OVER w_recent AS last7_max_units,
        CUME_DIST() OVER w_hist AS revenue_per_unit_cume_dist
    FROM base
    WINDOW w_recent AS (PARTITION BY store_id ORDER BY revenue_per_unit, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), w_hist AS (ORDER BY revenue_per_unit DESC NULLS FIRST)
)
-- Join a dimension and aggregate
SELECT
    dim.brand,
    dim.category,
    COUNT(*) AS num_rows,
    ROUND(SUM(s.revenue), 2) AS total_revenue
FROM with_windows s
LEFT JOIN products dim ON s.product_id = dim.id
GROUP BY ROLLUP (dim.brand, dim.category)
ORDER BY category DESC NULLS FIRST
