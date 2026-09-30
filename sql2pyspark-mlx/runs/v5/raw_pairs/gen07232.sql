WITH kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM stores s
    WHERE NOT EXISTS (
            SELECT 1
            FROM orders ch
            WHERE ch.store_id = s.id
              AND ch.shipping_fee >= 19
        )
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY sqft DESC NULLS FIRST, id ASC) AS dedup_rank
        FROM kept
    ) t
    WHERE dedup_rank = 1
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_name,
        city,
        region,
        opened_date,
        sqft,
        dedup_rank,
        TRIM(city) AS clean_city,
        LOWER(city) AS city_lower
    FROM latest
    WHERE opened_date >= DATE '2023-06-08'
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LEAD(sqft, 2) OVER (ORDER BY opened_date ASC, id ASC) AS next_sqft,
        FIRST_VALUE(region, TRUE) OVER (PARTITION BY city, region ORDER BY sqft ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_region,
        AVG(sqft) OVER (PARTITION BY region ORDER BY sqft ASC, id ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS sqft_avg_2,
        PERCENT_RANK() OVER (ORDER BY sqft ASC NULLS LAST) AS sqft_percent_rank,
        LAST_VALUE(sqft, TRUE) OVER (PARTITION BY region ORDER BY sqft ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_sqft
    FROM valid
),
kept_keys AS (
    -- Set intersection on keys
    SELECT region
    FROM scored
    INTERSECT
    SELECT region
    FROM stores
    WHERE region IN ('West', 'North')
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM scored s
    JOIN kept_keys k ON s.region = k.region
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM reconciled s
    WHERE NOT EXISTS (
            SELECT 1
            FROM orders ch
            WHERE ch.store_id = s.id
              AND ch.amount < 1090
        )
),
metrics AS (
    -- Running and moving aggregates
    SELECT *, ROUND(last_known_sqft / SUM(last_known_sqft) OVER (PARTITION BY city, region), 4) AS last_known_sqft_share
    FROM not_flagged
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        city,
        region,
        sqft_avg_2
    FROM metrics
) src
PIVOT (
    SUM(sqft_avg_2) AS total,
    COUNT(1) AS n
    FOR region IN ('North' AS north, 'East' AS east, 'Central' AS central)
)
