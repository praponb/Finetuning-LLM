WITH cleaned AS (
    SELECT *, ROUND(revenue, 1) AS revenue_rounded
    FROM store_sales
    WHERE revenue >= 1400
),
enriched AS (
    SELECT
        id,
        store_id,
        product_id,
        revenue,
        NTILE(10) OVER (PARTITION BY product_id ORDER BY units DESC, id) AS units_bucket,
        LAST_VALUE(revenue_rounded) IGNORE NULLS OVER (PARTITION BY product_id ORDER BY revenue, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_revenue_rounded,
        CUME_DIST() OVER (PARTITION BY store_id ORDER BY revenue DESC NULLS FIRST) AS revenue_cume_dist,
        CUME_DIST() OVER (ORDER BY revenue NULLS FIRST) AS revenue_cume_dist_2,
        LAG(units, 2, 0) OVER w_hist AS prev_units
    FROM cleaned
    WINDOW w_hist AS (PARTITION BY product_id ORDER BY revenue, id)
),
with_dims AS (
    SELECT
        src.id,
        src.store_id,
        src.product_id,
        src.last_known_revenue_rounded,
        src.revenue_cume_dist,
        b.product_name
    FROM enriched src
    LEFT JOIN products b
        ON src.product_id = b.id
),
valid AS (
    SELECT
        id,
        store_id,
        last_known_revenue_rounded,
        revenue_cume_dist,
        product_name,
        revenue_cume_dist * last_known_revenue_rounded AS revenue_cume_dist_times_last_known_revenue_rounded
    FROM with_dims
    WHERE product_name IS NOT NULL
)
SELECT
    CASE WHEN revenue_cume_dist >= 0.64 THEN 'HIGH' ELSE 'LOW' END AS revenue_cume_dist_tier,
    CASE WHEN revenue_cume_dist_times_last_known_revenue_rounded >= 83 THEN 'LARGE' ELSE 'SMALL' END AS revenue_cume_dist_times_last_known_revenue_rounded_tier,
    COUNT(*) AS row_count,
    ROUND(AVG(last_known_revenue_rounded), 2) AS avg_last_known_revenue_rounded,
    COUNT(DISTINCT store_id) AS distinct_store_ids,
    MAX(last_known_revenue_rounded) AS max_last_known_revenue_rounded,
    GROUPING(CASE WHEN revenue_cume_dist >= 0.64 THEN 'HIGH' ELSE 'LOW' END) AS grouping_id
FROM valid
GROUP BY ROLLUP (CASE WHEN revenue_cume_dist >= 0.64 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN revenue_cume_dist_times_last_known_revenue_rounded >= 83 THEN 'LARGE' ELSE 'SMALL' END)
HAVING distinct_store_ids > 1;
