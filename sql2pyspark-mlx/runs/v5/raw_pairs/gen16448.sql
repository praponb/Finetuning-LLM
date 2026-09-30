WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue,
        LEAST(units, revenue) AS least_units_revenue
    FROM store_sales
    WHERE sale_date >= TO_DATE('2023-09-13')
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY store_id, product_id ORDER BY sale_date DESC, id ASC) AS seq_no,
        AVG(units) OVER w_running AS units_avg_5_prior,
        RANK() OVER w_grp AS revenue_rank,
        AVG(revenue) OVER (PARTITION BY store_id ORDER BY least_units_revenue ASC, id ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS revenue_avg_3,
        NTILE(5) OVER w_grp_2 AS revenue_bucket
    FROM cleaned
    WINDOW w_running AS (PARTITION BY store_id ORDER BY least_units_revenue ASC, id ASC ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING), w_grp AS (PARTITION BY store_id ORDER BY revenue DESC), w_grp_2 AS (PARTITION BY store_id ORDER BY revenue ASC, id ASC)
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM with_windows
    QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY revenue_rank DESC NULLS LAST, id ASC) = 1
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        store_id,
        product_id,
        least_units_revenue,
        units_avg_5_prior,
        revenue_rank,
        revenue_bucket,
        SUM(least_units_revenue) OVER (PARTITION BY product_id ORDER BY revenue_avg_3 ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_least_units_revenue,
        NTILE(5) OVER (PARTITION BY product_id ORDER BY least_units_revenue DESC, id ASC) AS least_units_revenue_bucket,
        DENSE_RANK() OVER (PARTITION BY product_id ORDER BY revenue_avg_3 DESC NULLS LAST) AS revenue_avg_3_dense_rank
    FROM deduped
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        store_id,
        least_units_revenue,
        units_avg_5_prior,
        running_least_units_revenue,
        least_units_revenue_bucket,
        revenue_avg_3_dense_rank
    FROM (
        SELECT *, RANK() OVER (PARTITION BY product_id ORDER BY least_units_revenue DESC) AS dedup_rank
        FROM metrics
    ) t
    WHERE dedup_rank = 1
)
-- Aggregate with conditional metrics
SELECT
    store_id,
    CASE WHEN running_least_units_revenue >= 385 THEN 'LARGE' ELSE 'SMALL' END AS running_least_units_revenue_tier,
    CASE WHEN least_units_revenue_bucket >= 9 THEN 'HIGH' ELSE 'LOW' END AS least_units_revenue_bucket_tier,
    PERCENTILE_APPROX(units_avg_5_prior, 0.5) AS units_avg_5_prior_p50,
    COUNT(*) AS cnt,
    COUNT(*) AS num_rows
FROM latest
GROUP BY store_id, CASE WHEN running_least_units_revenue >= 385 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN least_units_revenue_bucket >= 9 THEN 'HIGH' ELSE 'LOW' END
ORDER BY cnt DESC;
