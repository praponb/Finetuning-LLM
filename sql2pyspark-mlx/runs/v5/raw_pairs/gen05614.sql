WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CONCAT(city, ' ', store_name) AS city_store_name,
        COALESCE(region, 'DIRECT') AS region_clean
    FROM stores
    WHERE region IS DISTINCT FROM 'Central'
      AND region IS NOT NULL
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM cleaned s
    WHERE NOT EXISTS (
            SELECT 1
            FROM orders ch
            WHERE ch.store_id = s.id
              AND ch.discount >= 0.3
        )
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        store_name,
        city,
        sqft,
        region_clean,
        MAX(sqft) OVER w_part AS last7_max_sqft,
        MIN(sqft) OVER w_part AS last7_min_sqft,
        COUNT(sqft) OVER w_part AS last7_count_sqft
    FROM kept
    WINDOW w_part AS (PARTITION BY city ORDER BY sqft, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        store_name,
        sqft,
        region_clean,
        last7_max_sqft,
        last7_min_sqft,
        last7_count_sqft,
        ROUND(sqft / SUM(sqft) OVER (PARTITION BY city), 4) AS sqft_share,
        SUM(last7_min_sqft) OVER (PARTITION BY city ORDER BY sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_last7_min_sqft,
        RANK() OVER (PARTITION BY city ORDER BY last7_min_sqft DESC) AS last7_min_sqft_rank,
        AVG(last7_min_sqft) OVER (PARTITION BY city ORDER BY last7_max_sqft, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS last7_min_sqft_avg_5
    FROM metrics
    WHERE store_name LIKE '%Riversid%'
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN last7_count_sqft >= 16 THEN 'LARGE' ELSE 'SMALL' END AS last7_count_sqft_tier,
    AVG(last7_max_sqft) AS avg_last7_max_sqft,
    SUM(sqft) AS total_sqft,
    STDDEV_SAMP(sqft) AS sqft_stddev
FROM scored
GROUP BY CASE WHEN last7_count_sqft >= 16 THEN 'LARGE' ELSE 'SMALL' END;
