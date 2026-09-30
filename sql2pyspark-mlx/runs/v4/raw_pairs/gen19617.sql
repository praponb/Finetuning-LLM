WITH with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        CUME_DIST() OVER (PARTITION BY store_id ORDER BY units DESC) AS units_cume_dist,
        AVG(units) OVER (PARTITION BY store_id, product_id ORDER BY revenue, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS units_avg_2_prior,
        ROUND(revenue / SUM(revenue) OVER (PARTITION BY store_id, product_id), 4) AS revenue_share
    FROM store_sales
)
SELECT
    id,
    store_id,
    product_id,
    units,
    revenue,
    units_avg_2_prior,
    revenue_share
FROM with_windows
ORDER BY store_id
