WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        store_name,
        city,
        region,
        opened_date,
        sqft,
        ROUND(sqft * 0.85, 2) AS sqft_scaled,
        TRIM(region) AS clean_region,
        COALESCE(region, 'N/A') AS region_clean
    FROM stores
    WHERE region IS NOT NULL
      AND region IS DISTINCT FROM 'West'
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        MIN(sqft) OVER (PARTITION BY city ORDER BY sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_sqft,
        MAX(sqft) OVER (PARTITION BY city ORDER BY sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_sqft,
        AVG(sqft) OVER (PARTITION BY city ORDER BY sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_sqft,
        AVG(sqft) OVER (PARTITION BY region ORDER BY opened_date, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS sqft_avg_2_prior
    FROM valid
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT city
    FROM metrics
    INTERSECT ALL
    SELECT city
    FROM stores
    WHERE region = 'East'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM metrics s
    INNER JOIN remaining_keys k ON s.city = k.city
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_name,
        sqft_scaled,
        clean_region,
        region_clean,
        running_min_sqft,
        running_max_sqft,
        running_avg_sqft,
        sqft_avg_2_prior,
        sqft_avg_2_prior * running_max_sqft AS sqft_avg_2_prior_times_running_max_sqft,
        COALESCE(region, 'OTHER') AS region_filled
    FROM kept_rows
    WHERE region IS NOT NULL
)
SELECT
    id,
    sqft_scaled,
    region_clean,
    running_min_sqft,
    running_max_sqft,
    running_avg_sqft,
    sqft_avg_2_prior,
    sqft_avg_2_prior_times_running_max_sqft,
    region_filled
FROM base
ORDER BY sqft_avg_2_prior_times_running_max_sqft
