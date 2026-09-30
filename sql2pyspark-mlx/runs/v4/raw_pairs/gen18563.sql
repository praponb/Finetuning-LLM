WITH enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MAX(sqft) OVER (PARTITION BY city) AS city_max_sqft,
        AVG(sqft) OVER (PARTITION BY region) AS region_avg_sqft,
        NTILE(3) OVER (PARTITION BY city, region ORDER BY sqft DESC, id) AS sqft_bucket
    FROM stores
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(city_max_sqft) OVER (ORDER BY region_avg_sqft, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS city_max_sqft_avg_5,
        FIRST_VALUE(sqft, TRUE) OVER (PARTITION BY city, region ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_sqft
    FROM enriched
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM scored s
    WHERE NOT EXISTS (
            SELECT 1
            FROM store_sales c
            WHERE c.store_id = s.id
              AND c.sale_date < s.opened_date
        )
)
SELECT
    id,
    region,
    opened_date,
    city_max_sqft,
    region_avg_sqft,
    city_max_sqft_avg_5,
    first_known_sqft,
    CASE WHEN region RLIKE '^(Central|South)$' THEN 'EXTENDED' ELSE 'PRIORITY' END AS region_zone
FROM not_flagged
ORDER BY region, id DESC
