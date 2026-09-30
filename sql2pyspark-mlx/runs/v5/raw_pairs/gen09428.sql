WITH base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        TRIM(store_name) AS clean_store_name,
        CASE WHEN sqft >= 14764 THEN 'High' WHEN sqft >= 4051 THEN 'Medium' ELSE 'Low' END AS sqft_tier,
        COALESCE(region, 'UNKNOWN') AS region_filled
    FROM stores
    WHERE YEAR(opened_date) >= 2023
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        store_name,
        city,
        region,
        sqft,
        region_filled,
        CONCAT_WS(',', city, store_name) AS city_store_name_key,
        COALESCE(region, 'N/A') AS region_clean
    FROM base
    WHERE region IS NOT NULL
      AND opened_date <= '2023-04-01'
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        store_name,
        region,
        sqft,
        city_store_name_key,
        CAST(sqft AS STRING) AS sqft_str,
        COALESCE(region, 'UNKNOWN') AS resolved_region
    FROM cleaned
    WHERE region IS NOT NULL
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        ROUND(sqft * 0.9, 2) AS adjusted_sqft,
        UPPER(TRIM(store_name)) AS store_name_normalized
    FROM filtered
    WHERE region IS NOT NULL
      AND region IS NOT DISTINCT FROM 'Central'
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN sqft >= 16545 THEN 'LARGE' ELSE 'SMALL' END AS sqft_tier,
        CASE WHEN adjusted_sqft >= 74 THEN 'PREMIUM' ELSE 'STANDARD' END AS adjusted_sqft_tier,
        STDDEV_SAMP(sqft) AS sqft_stddev,
        COUNT(*) AS event_count,
        COUNT(DISTINCT id) AS distinct_ids,
        MIN(adjusted_sqft) AS min_adjusted_sqft,
        COUNT(*) AS cnt
    FROM valid
    GROUP BY CASE WHEN sqft >= 16545 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN adjusted_sqft >= 74 THEN 'PREMIUM' ELSE 'STANDARD' END
)
SELECT
    sqft_tier,
    adjusted_sqft_tier,
    sqft_stddev,
    event_count
FROM stats
ORDER BY event_count, adjusted_sqft_tier;
