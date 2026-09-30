WITH scored AS (
    SELECT
        id,
        store_name,
        city,
        region,
        opened_date,
        sqft,
        FIRST_VALUE(region) IGNORE NULLS OVER (ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_region,
        FIRST_VALUE(region, TRUE) OVER w_hist first_known_region_2,
        NTILE(10) OVER w_recent sqft_bucket,
        FIRST_VALUE(region) IGNORE NULLS OVER w_hist first_known_region_3,
        ROW_NUMBER() OVER w_recent seq_no
    FROM stores
    WINDOW w_hist AS (PARTITION BY city ORDER BY sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_recent AS (PARTITION BY region ORDER BY sqft DESC, id)
),
scored_stores AS (
    SELECT
        id,
        store_name,
        city,
        region,
        opened_date,
        first_known_region,
        sqft_bucket,
        MAX(sqft_bucket) OVER (PARTITION BY region ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_max_sqft_bucket,
        AVG(sqft_bucket) OVER (PARTITION BY region ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_avg_sqft_bucket,
        MIN(sqft_bucket) OVER (PARTITION BY region ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_min_sqft_bucket,
        COUNT(sqft_bucket) OVER (PARTITION BY region ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_count_sqft_bucket,
        LAST_VALUE(region) IGNORE NULLS OVER (PARTITION BY region ORDER BY sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_region
    FROM scored
),
not_flagged AS (
    SELECT s.*
    FROM scored_stores s
    WHERE NOT EXISTS (
            SELECT 1
            FROM orders x
            WHERE x.store_id = s.id
              AND x.order_date <= s.opened_date
        )
),
enriched AS (
    SELECT
        id,
        store_name,
        city,
        region,
        sqft_bucket,
        running_max_sqft_bucket,
        running_avg_sqft_bucket,
        NTILE(5) OVER (PARTITION BY region, city ORDER BY running_count_sqft_bucket DESC, id) running_count_sqft_bucket_bucket,
        CUME_DIST() OVER (PARTITION BY region ORDER BY running_count_sqft_bucket DESC) running_count_sqft_bucket_cume_dist,
        ROUND(running_min_sqft_bucket / SUM(running_min_sqft_bucket) OVER (PARTITION BY region, city), 4) running_min_sqft_bucket_share,
        MIN(running_min_sqft_bucket) OVER (PARTITION BY region) region_min_running_min_sqft_bucket
    FROM not_flagged
    WHERE store_name LIKE '%Mall%'
)
SELECT
    region,
    COUNT(*) AS row_count,
    COUNT_IF(region = 'Central') AS central_events,
    COUNT(*) AS row_count_2
FROM enriched
GROUP BY region
ORDER BY row_count_2;
