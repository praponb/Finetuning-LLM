WITH filtered AS (
    SELECT
        *,
        FLOOR(sqft) sqft_whole,
        INITCAP(TRIM(region)) region_title
    FROM stores
    WHERE opened_date >= DATE '2023-08-02'
      AND region IS DISTINCT FROM 'North'
),
with_windows AS (
    SELECT
        *,
        MAX(sqft) OVER (PARTITION BY city, region ORDER BY opened_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) last7_max_sqft,
        MIN(sqft) OVER (PARTITION BY city, region ORDER BY opened_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) last7_min_sqft,
        COUNT(sqft) OVER (PARTITION BY city, region ORDER BY opened_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) last7_count_sqft,
        SUM(sqft) OVER (PARTITION BY city, region ORDER BY opened_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) last7_sum_sqft,
        AVG(sqft_whole) OVER (PARTITION BY city ORDER BY sqft, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) sqft_whole_avg_5_prior
    FROM filtered
),
eligible AS (
    SELECT s.*
    FROM with_windows s
    WHERE EXISTS (
            SELECT 1
            FROM orders c
            WHERE c.store_id = s.id
              AND c.order_date >= '2025-04-01'
        )
),
deduped AS (
    SELECT *
    FROM eligible
    QUALIFY ROW_NUMBER() OVER (PARTITION BY region ORDER BY last7_sum_sqft, id) = 1
),
enriched AS (
    SELECT
        *,
        NTILE(4) OVER (PARTITION BY region ORDER BY last7_sum_sqft, id) last7_sum_sqft_bucket,
        LAST_VALUE(sqft_whole) IGNORE NULLS OVER (PARTITION BY city ORDER BY last7_max_sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_sqft_whole
    FROM deduped
    WHERE last7_max_sqft <= 4500
)
SELECT
    id,
    store_name,
    city,
    opened_date,
    sqft_whole,
    region_title,
    last7_min_sqft,
    last7_count_sqft,
    sqft_whole_avg_5_prior,
    CASE WHEN city RLIKE '^(New York|Tokyo)$' THEN 'OTHER_ZONE' ELSE 'PRIORITY' END AS city_zone
FROM enriched
ORDER BY id DESC
