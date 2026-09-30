WITH with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        store_name,
        city,
        region,
        opened_date,
        sqft,
        LAST_VALUE(sqft, TRUE) OVER (PARTITION BY region ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_sqft,
        AVG(sqft) OVER (PARTITION BY region ORDER BY sqft, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS sqft_avg_3
    FROM stores
),
matched_keys AS (
    -- Set difference on keys
    SELECT region, city
    FROM with_windows
    EXCEPT
    SELECT region, city
    FROM stores
    WHERE store_name LIKE 'M%'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_windows s
    INNER JOIN matched_keys k ON s.region = k.region AND s.city = k.city
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(sqft / SUM(sqft) OVER (PARTITION BY city), 4) AS sqft_share,
        CUME_DIST() OVER (ORDER BY sqft_avg_3 DESC NULLS FIRST) AS sqft_avg_3_cume_dist,
        DENSE_RANK() OVER (PARTITION BY city ORDER BY sqft_avg_3 DESC NULLS FIRST) AS sqft_avg_3_dense_rank
    FROM verified
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY sqft_avg_3_cume_dist DESC NULLS LAST) AS sqft_avg_3_cume_dist_dense_rank,
        ROUND(sqft / SUM(sqft) OVER (PARTITION BY city, region), 4) AS sqft_share_2,
        ROUND(last_known_sqft / SUM(last_known_sqft) OVER (PARTITION BY region, city), 4) AS last_known_sqft_share
    FROM metrics
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM enriched s
    WHERE EXISTS (
            SELECT 1
            FROM inventory c
            WHERE c.store_id = s.id
              AND c.last_restock BETWEEN '2023-03-01' AND '2024-01-24'
        )
)
-- Grouped metrics
SELECT
    CASE WHEN sqft_share_2 >= 0.81 THEN 'LARGE' ELSE 'SMALL' END AS sqft_share_2_tier,
    DATE_TRUNC('WEEK', opened_date) AS week_start,
    city,
    COUNT(DISTINCT region) AS distinct_regions,
    SUM(last_known_sqft) FILTER (WHERE city = 'Singapore') AS singapore_total_last_known_sqft,
    SIZE(COLLECT_SET(city)) AS num_cities
FROM kept
GROUP BY sqft_share_2_tier, week_start, city
ORDER BY week_start NULLS FIRST;
