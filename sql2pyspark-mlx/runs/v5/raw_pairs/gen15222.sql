WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_name,
        region,
        opened_date,
        sqft,
        LOWER(region) AS region_lower
    FROM stores
    WHERE region IS NOT NULL
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM valid s
    WHERE EXISTS (
            SELECT 1
            FROM inventory x
            WHERE x.store_id = s.id
              AND x.last_restock <= s.opened_date
        )
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        store_name,
        RANK() OVER (ORDER BY sqft DESC NULLS FIRST) AS sqft_rank,
        LEAD(sqft, 1, 0) OVER (PARTITION BY region ORDER BY sqft, id) AS next_sqft,
        SUM(sqft) OVER (PARTITION BY region ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sqft,
        LEAD(sqft, 1) OVER (PARTITION BY region ORDER BY opened_date, id) AS next_sqft_2,
        NTILE(5) OVER (PARTITION BY region ORDER BY sqft, id) AS sqft_bucket
    FROM kept
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM scored s
    WHERE NOT EXISTS (
            SELECT 1
            FROM inventory c
            WHERE c.store_id = s.id
              AND c.product_id IN (4, 6)
        )
)
-- Multi-level aggregation
SELECT
    CASE WHEN sqft_bucket >= 8 THEN 'LARGE' ELSE 'SMALL' END AS sqft_bucket_tier,
    COUNT(DISTINCT id) AS distinct_ids,
    AVG(next_sqft_2) AS avg_next_sqft_2,
    COUNT(*) AS event_count
FROM eligible
GROUP BY 1
ORDER BY distinct_ids DESC NULLS LAST
