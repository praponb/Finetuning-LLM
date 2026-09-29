WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CONCAT_WS(',', city, region) AS city_region_key
    FROM stores
    WHERE opened_date > DATE '2024-01-20'
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY opened_date DESC, id ASC) AS latest_rank
        FROM valid
    ) ranked
    WHERE latest_rank = 1
),
remaining_keys AS (
    -- Set difference on keys
    SELECT id
    FROM deduped
    EXCEPT ALL
    SELECT id
    FROM stores
    WHERE opened_date > '2023-11-01'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM deduped s
    INNER JOIN remaining_keys k ON s.id = k.id
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        RANK() OVER (ORDER BY sqft ASC) AS sqft_rank,
        MIN(sqft) OVER (PARTITION BY city) AS city_min_sqft,
        ROUND(sqft / SUM(sqft) OVER (PARTITION BY region), 4) AS sqft_share,
        DENSE_RANK() OVER (ORDER BY sqft DESC) AS sqft_dense_rank
    FROM kept_rows
),
remaining_keys_2 AS (
    -- Set difference on keys
    SELECT id, city
    FROM metrics
    EXCEPT
    SELECT id, city
    FROM stores
    WHERE region IN ('North', 'West')
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM metrics s
    INNER JOIN remaining_keys_2 k ON s.id = k.id AND s.city = k.city
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY city_min_sqft DESC) AS city_min_sqft_dense_rank,
        DENSE_RANK() OVER (PARTITION BY region ORDER BY sqft DESC) AS sqft_dense_rank_2,
        MAX(sqft_dense_rank) OVER (PARTITION BY city) AS city_max_sqft_dense_rank
    FROM verified
)
-- Grouped metrics
SELECT
    CASE WHEN city_max_sqft_dense_rank >= 7 THEN 'LARGE' ELSE 'SMALL' END AS city_max_sqft_dense_rank_tier,
    COUNT(*) AS row_count,
    ROUND(AVG(sqft), 2) AS avg_sqft,
    COUNT(*) AS row_count_2,
    COUNT(DISTINCT city) AS distinct_cities
FROM with_windows
GROUP BY ROLLUP (CASE WHEN city_max_sqft_dense_rank >= 7 THEN 'LARGE' ELSE 'SMALL' END);
