WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        store_name,
        city,
        region,
        opened_date,
        sqft,
        LAST_DAY(opened_date) AS opened_date_month_end,
        COALESCE(region, 'DIRECT') AS resolved_region
    FROM stores
    WHERE UPPER(TRIM(store_name)) = 'OLD TOWN'
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY region ORDER BY sqft NULLS LAST, id) AS latest_rank
        FROM base
    ) x
    WHERE latest_rank = 1
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        sqft * latest_rank AS sqft_times_latest_rank,
        ADD_MONTHS(opened_date, 3) AS opened_date_plus_months,
        COALESCE(region, 'OTHER') AS region_filled
    FROM ranked
    WHERE store_name NOT LIKE 'Dow%'
),
ranked_stores AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city, region ORDER BY opened_date_plus_months NULLS FIRST, id) AS latest_rank_2
    FROM cleaned
    QUALIFY latest_rank_2 = 1
),
kept_keys AS (
    -- Set intersection on keys
    SELECT city
    FROM ranked_stores
    INTERSECT
    SELECT city
    FROM stores
    WHERE opened_date > '2025-10-18'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM ranked_stores s
    INNER JOIN kept_keys k ON s.city = k.city
),
grouped AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN sqft >= 14002 THEN 'HIGH' ELSE 'LOW' END AS sqft_tier,
        ROUND(AVG(sqft_times_latest_rank), 2) AS avg_sqft_times_latest_rank,
        COUNT(*) AS num_rows
    FROM reconciled
    GROUP BY CASE WHEN sqft >= 14002 THEN 'HIGH' ELSE 'LOW' END
)
SELECT
    sqft_tier,
    avg_sqft_times_latest_rank,
    num_rows
FROM grouped
