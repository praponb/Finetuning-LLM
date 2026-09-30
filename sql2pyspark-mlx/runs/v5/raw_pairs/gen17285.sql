WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        store_name,
        region,
        opened_date,
        sqft,
        INITCAP(TRIM(store_name)) AS store_name_title,
        COALESCE(region, 'DIRECT') AS resolved_region
    FROM stores
    WHERE region IS NOT NULL
),
kept_keys AS (
    -- Set intersection on keys
    SELECT region, id
    FROM base
    INTERSECT ALL
    SELECT region, id
    FROM stores
    WHERE region IN ('South', 'East', 'Central')
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM base s
    INNER JOIN kept_keys k ON s.region = k.region AND s.id = k.id
),
scored AS (
    -- Running and moving aggregates
    SELECT *, AVG(sqft) OVER (PARTITION BY region) AS region_avg_sqft
    FROM reconciled
    WHERE store_name LIKE '%Flagship%'
),
scored_stores AS (
    -- Ranking and gap features
    SELECT
        *,
        RANK() OVER (PARTITION BY region ORDER BY region_avg_sqft DESC NULLS LAST) AS region_avg_sqft_rank,
        RANK() OVER (PARTITION BY region ORDER BY region_avg_sqft ASC NULLS LAST) AS region_avg_sqft_rank_2,
        SUM(region_avg_sqft) OVER (PARTITION BY region) AS region_sum_region_avg_sqft
    FROM scored
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        region_sum_region_avg_sqft * region_avg_sqft AS region_sum_region_avg_sqft_times_region_avg_sqft,
        ROUND(region_avg_sqft / region_sum_region_avg_sqft, 2) AS region_avg_sqft_per_region_sum_region_avg_sqft,
        COALESCE(region, 'OTHER') AS region_clean
    FROM scored_stores
    WHERE region IS NOT NULL
),
valid_stores AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        COALESCE(region_sum_region_avg_sqft_times_region_avg_sqft, 0) AS region_sum_region_avg_sqft_times_region_avg_sqft_filled,
        DATE_TRUNC('year', opened_date) AS opened_date_period
    FROM valid
    WHERE region IS DISTINCT FROM 'West'
      AND region_sum_region_avg_sqft_times_region_avg_sqft <= 23
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM valid_stores s
    WHERE NOT EXISTS (
            SELECT 1
            FROM orders x
            WHERE x.store_id = s.id
              AND x.order_date >= s.opened_date_period
        )
),
grouped AS (
    -- Grouped metrics
    SELECT
        DATE_TRUNC('WEEK', opened_date) AS week_start,
        SUM(CASE WHEN region = 'East' THEN region_sum_region_avg_sqft ELSE 0 END) AS east_region_sum_region_avg_sqft,
        MAX(opened_date_period) AS last_seen,
        PERCENTILE_APPROX(region_avg_sqft, 0.5) AS region_avg_sqft_p50,
        BIT_OR(sqft) AS sqft_bit_or,
        STDDEV_SAMP(sqft) AS sqft_stddev
    FROM eligible
    GROUP BY DATE_TRUNC('WEEK', opened_date)
)
SELECT
    week_start,
    east_region_sum_region_avg_sqft,
    sqft_bit_or
FROM grouped
ORDER BY week_start ASC, east_region_sum_region_avg_sqft ASC;
