WITH enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue,
        LEAD(revenue, 1) OVER (PARTITION BY store_id ORDER BY sale_date, id) next_revenue,
        AVG(units) OVER (PARTITION BY product_id ORDER BY units, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) units_avg_2,
        NTILE(10) OVER (PARTITION BY product_id ORDER BY revenue DESC, id) revenue_bucket
    FROM store_sales
),
with_dims AS (
    -- Attach stores attributes
    SELECT
        e.*,
        d.sqft,
        d.city
    FROM enriched e
    LEFT JOIN stores d
        ON e.store_id = d.id
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY revenue_bucket DESC NULLS LAST) revenue_bucket_dense_rank,
        SUM(units_avg_2) OVER (PARTITION BY store_id ORDER BY next_revenue, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_units_avg_2,
        SUM(revenue) OVER (ORDER BY sale_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_revenue,
        PERCENT_RANK() OVER (PARTITION BY product_id ORDER BY next_revenue NULLS FIRST) next_revenue_percent_rank,
        ROUND(units / SUM(units) OVER (PARTITION BY store_id), 4) units_share
    FROM with_dims
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY units_avg_2 DESC NULLS LAST, id) AS row_num
    FROM with_windows
    QUALIFY row_num = 1
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN revenue_bucket_dense_rank >= 9 THEN 'LARGE' ELSE 'SMALL' END AS revenue_bucket_dense_rank_tier,
        BIT_AND(units) AS units_bit_and,
        SUM(units) AS total_units,
        SIZE(COLLECT_SET(city)) AS num_cities
    FROM deduped
    GROUP BY CASE WHEN revenue_bucket_dense_rank >= 9 THEN 'LARGE' ELSE 'SMALL' END
    HAVING BIT_AND(units) >= 1
)
SELECT revenue_bucket_dense_rank_tier, units_bit_and
FROM summary
