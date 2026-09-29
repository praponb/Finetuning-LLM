WITH enriched AS (
    SELECT
        *,
        AVG(units) OVER (PARTITION BY product_id, store_id) AS product_id_avg_units,
        AVG(revenue) OVER (ORDER BY units, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS revenue_avg_3_prior
    FROM store_sales
),
metrics AS (
    SELECT
        *,
        LEAD(units, 1, 0) OVER (PARTITION BY store_id ORDER BY revenue_avg_3_prior, id) AS next_units,
        ROUND(units / SUM(units) OVER (PARTITION BY store_id), 4) AS units_share,
        NTILE(10) OVER (ORDER BY revenue, id) AS revenue_bucket,
        COUNT(revenue_avg_3_prior) OVER w_part AS product_id_count_revenue_avg_3_prior,
        ROUND(units / SUM(units) OVER (PARTITION BY store_id), 4) AS units_share_2
    FROM enriched
    WINDOW w_part AS (PARTITION BY product_id, store_id)
),
latest AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY units_share DESC NULLS LAST, id) AS dedup_rank
        FROM metrics
    ) x
    WHERE dedup_rank = 1
),
with_dims AS (
    SELECT s.*, p.launch_date
    FROM latest s
    LEFT JOIN products p
        ON s.product_id = p.id
),
latest_store_sales AS (
    SELECT *
    FROM with_dims
    QUALIFY ROW_NUMBER() OVER (PARTITION BY store_id, product_id ORDER BY next_units DESC NULLS FIRST, id) = 1
),
summary AS (
    SELECT
        CASE WHEN product_id_count_revenue_avg_3_prior >= 13 THEN 'HIGH' ELSE 'LOW' END AS product_id_count_revenue_avg_3_prior_tier,
        CASE WHEN revenue_avg_3_prior >= 3348 THEN 'LARGE' ELSE 'SMALL' END AS revenue_avg_3_prior_tier,
        PERCENTILE_APPROX(next_units, ARRAY(0.25, 0.5, 0.75)) AS next_units_quartiles,
        CORR(revenue, units) AS corr_revenue_units,
        STDDEV_SAMP(revenue) AS revenue_stddev
    FROM latest_store_sales
    GROUP BY CUBE (CASE WHEN product_id_count_revenue_avg_3_prior >= 13 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN revenue_avg_3_prior >= 3348 THEN 'LARGE' ELSE 'SMALL' END)
)
SELECT
    product_id_count_revenue_avg_3_prior_tier,
    revenue_avg_3_prior_tier,
    next_units_quartiles,
    corr_revenue_units
FROM summary
