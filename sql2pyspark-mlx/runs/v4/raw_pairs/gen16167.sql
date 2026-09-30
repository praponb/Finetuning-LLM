WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_name,
        city,
        region,
        sqft,
        LAST_DAY(opened_date) AS opened_date_month_end
    FROM stores
    WHERE region IS NOT NULL
),
kept_keys AS (
    -- Set difference on keys
    SELECT region, id
    FROM filtered
    EXCEPT ALL
    SELECT region, id
    FROM stores
    WHERE sqft >= 8251
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM filtered s
    INNER JOIN kept_keys k ON s.region = k.region AND s.id = k.id
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        RANK() OVER (ORDER BY sqft DESC NULLS FIRST) AS sqft_rank,
        ROUND(sqft / SUM(sqft) OVER (PARTITION BY region), 4) AS sqft_share,
        COUNT(sqft) OVER (PARTITION BY city, region) AS city_count_sqft,
        COUNT(sqft) OVER (PARTITION BY region) AS region_count_sqft
    FROM kept_rows
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM scored s
    WHERE EXISTS (
            SELECT 1
            FROM orders x
            WHERE x.store_id = s.id
              AND x.customer_id IS NULL
        )
)
-- Running and moving aggregates
SELECT
    id,
    store_name,
    city,
    region,
    sqft,
    opened_date_month_end,
    sqft_rank,
    sqft_share,
    city_count_sqft,
    region_count_sqft,
    DENSE_RANK() OVER (PARTITION BY city, region ORDER BY sqft NULLS LAST) AS sqft_dense_rank,
    CUME_DIST() OVER (PARTITION BY region ORDER BY city_count_sqft DESC) AS city_count_sqft_cume_dist,
    DENSE_RANK() OVER (ORDER BY city_count_sqft NULLS FIRST) AS city_count_sqft_dense_rank,
    PERCENT_RANK() OVER (PARTITION BY city, region ORDER BY sqft NULLS LAST) AS sqft_percent_rank,
    MIN(sqft_share) OVER (PARTITION BY city) AS city_min_sqft_share
FROM eligible
