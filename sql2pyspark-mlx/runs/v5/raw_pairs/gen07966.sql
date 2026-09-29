WITH base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        LENGTH(region) AS region_length,
        CASE WHEN region IN ('North', 'Central') THEN 'Core' ELSE 'Other' END AS region_group
    FROM stores
    WHERE region IS NOT NULL
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        store_name,
        sqft,
        region_length,
        NTILE(4) OVER (PARTITION BY region ORDER BY sqft DESC, id) AS sqft_bucket,
        SUM(region_length) OVER (ORDER BY region_length, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_region_length,
        NTILE(3) OVER (PARTITION BY region, city ORDER BY region_length DESC, id) AS region_length_bucket
    FROM base
),
metrics_stores AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        sqft,
        sqft_bucket,
        running_region_length,
        region_length_bucket,
        MIN(region_length_bucket) OVER w_part AS running_min_region_length_bucket,
        MAX(region_length_bucket) OVER w_part AS running_max_region_length_bucket,
        COUNT(region_length_bucket) OVER w_part AS running_count_region_length_bucket,
        LAG(running_region_length, 1) OVER (ORDER BY sqft, id) AS prev_running_region_length
    FROM metrics
    WINDOW w_part AS (ORDER BY region_length_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(region_length_bucket) OVER w_running AS region_length_bucket_avg_3_prior,
        LEAD(sqft_bucket, 2, 0) OVER (ORDER BY running_count_region_length_bucket, id) AS next_sqft_bucket,
        LEAD(running_min_region_length_bucket, 1) OVER (ORDER BY running_max_region_length_bucket, id) AS next_running_min_region_length_bucket
    FROM metrics_stores
    WINDOW w_running AS (ORDER BY running_max_region_length_bucket, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING)
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM enriched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM store_sales c
            WHERE c.store_id = s.id
              AND c.revenue > 2900
        )
)
SELECT
    id,
    sqft,
    sqft_bucket,
    running_region_length,
    region_length_bucket,
    running_min_region_length_bucket,
    region_length_bucket_avg_3_prior,
    next_sqft_bucket,
    next_running_min_region_length_bucket
FROM eligible
ORDER BY region_length_bucket NULLS FIRST, id;
