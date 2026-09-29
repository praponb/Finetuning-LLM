WITH enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        CUME_DIST() OVER (ORDER BY units NULLS FIRST) AS units_cume_dist,
        PERCENT_RANK() OVER (PARTITION BY store_id ORDER BY revenue DESC NULLS LAST) AS revenue_percent_rank,
        LAST_VALUE(revenue) IGNORE NULLS OVER w_part AS last_known_revenue
    FROM store_sales
    WINDOW w_part AS (PARTITION BY product_id ORDER BY revenue, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
-- Grouped metrics
SELECT
    product_id,
    COUNT(DISTINCT id) AS distinct_ids,
    STDDEV_SAMP(units) AS units_stddev,
    ROUND(AVG(revenue_percent_rank), 2) AS avg_revenue_percent_rank,
    PERCENTILE_APPROX(revenue_percent_rank, ARRAY(0.25, 0.5, 0.75)) AS revenue_percent_rank_quartiles,
    PERCENTILE_APPROX(units, ARRAY(0.25, 0.5, 0.75)) AS units_quartiles
FROM enriched
GROUP BY product_id
