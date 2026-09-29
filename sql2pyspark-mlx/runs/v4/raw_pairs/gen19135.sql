WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_name,
        city,
        region,
        opened_date,
        sqft,
        CONCAT_WS(',', city, store_name) AS city_store_name_key,
        COALESCE(region, 'UNKNOWN') AS resolved_region
    FROM stores
    WHERE opened_date >= '2024-12-01'
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        STDDEV_SAMP(sqft) OVER (PARTITION BY city) AS city_stddev_samp_sqft,
        DENSE_RANK() OVER (PARTITION BY city ORDER BY sqft NULLS FIRST) AS sqft_dense_rank
    FROM cleaned
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        store_name,
        city,
        region,
        opened_date,
        sqft,
        city_store_name_key,
        resolved_region,
        city_stddev_samp_sqft,
        DATE_SUB(opened_date, 5) AS opened_date_minus_5d
    FROM scored
    WHERE opened_date > DATE '2023-09-12'
      AND region IS NOT NULL
),
base_stores AS (
    -- Keep recent, valid rows
    SELECT
        *,
        FLOOR(sqft) AS sqft_whole,
        COALESCE(region, 'DIRECT') AS region_filled
    FROM base
    WHERE opened_date > DATE '2023-03-06'
      AND city = 'Berlin'
),
kept_keys AS (
    -- Set intersection on keys
    SELECT region, id
    FROM base_stores
    INTERSECT ALL
    SELECT region, id
    FROM stores
    WHERE region = 'West'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM base_stores s
    INNER JOIN kept_keys k ON s.region = k.region AND s.id = k.id
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM verified s
    WHERE NOT EXISTS (
            SELECT 1
            FROM inventory c
            WHERE c.store_id = s.id
              AND c.last_restock < s.opened_date
        )
),
daily AS (
    -- Aggregate with conditional metrics
    SELECT
        city,
        COUNT(DISTINCT id) AS distinct_ids,
        APPROX_PERCENTILE(sqft_whole, 0.9) AS sqft_whole_p90,
        COUNT(CASE WHEN city = 'Chiang Mai' THEN 1 END) AS chiang_mai_count,
        ROUND(SUM(sqft) / NULLIF(COUNT(CASE WHEN region = 'East' THEN 1 END), 0), 2) AS sqft_per_east,
        PERCENTILE_APPROX(sqft_whole, 0.9) AS sqft_whole_p90_2
    FROM eligible
    GROUP BY city
    HAVING distinct_ids >= 1
)
SELECT
    city,
    distinct_ids,
    sqft_per_east
FROM daily
ORDER BY sqft_per_east DESC NULLS FIRST;
