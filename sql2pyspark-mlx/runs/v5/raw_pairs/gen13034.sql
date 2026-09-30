WITH scored AS (
    SELECT
        *,
        RANK() OVER (ORDER BY revenue DESC) AS revenue_rank,
        CORR(units, revenue) OVER (PARTITION BY product_id, store_id) AS corr_units_revenue,
        AVG(units) OVER (PARTITION BY product_id ORDER BY revenue ASC, id ASC ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS units_avg_6_prior,
        RANK() OVER (PARTITION BY product_id ORDER BY revenue ASC NULLS FIRST) AS revenue_rank_2,
        NTILE(4) OVER (PARTITION BY store_id ORDER BY units ASC, id ASC) AS units_bucket
    FROM store_sales
)
SELECT
    CASE WHEN revenue >= 3600 THEN 'PREMIUM' ELSE 'STANDARD' END AS revenue_tier,
    DATE_TRUNC('WEEK', sale_date) AS week_start,
    store_id,
    PERCENTILE_APPROX(corr_units_revenue, ARRAY(0.25, 0.5, 0.75)) AS corr_units_revenue_quartiles,
    COUNT(DISTINCT id) AS distinct_ids,
    GROUPING(CASE WHEN revenue >= 3600 THEN 'PREMIUM' ELSE 'STANDARD' END) AS grouping_id
FROM scored
GROUP BY ROLLUP (CASE WHEN revenue >= 3600 THEN 'PREMIUM' ELSE 'STANDARD' END, DATE_TRUNC('WEEK', sale_date), store_id)
HAVING COUNT(DISTINCT id) > 1
