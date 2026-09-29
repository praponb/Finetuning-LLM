WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        FLOOR(sqft) AS sqft_whole,
        CONCAT(region, ', ', city) AS region_city,
        COALESCE(region, 'N/A') AS region_clean
    FROM stores
    WHERE YEAR(opened_date) = 2025
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        city,
        region,
        opened_date,
        sqft_whole,
        region_city,
        FLOOR(sqft) AS sqft_whole_2,
        COALESCE(region, 'DIRECT') AS region_filled
    FROM filtered
    WHERE opened_date > TO_DATE('2023-04-11')
      AND region IS NOT NULL
),
valid_stores AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, LOWER(city) AS city_lower
    FROM valid
    WHERE sqft_whole_2 >= 26
),
base AS (
    -- Basic cleansing and defaults
    SELECT *, DATE_FORMAT(opened_date, 'MMM yyyy') AS opened_date_label
    FROM valid_stores
    WHERE opened_date >= DATE '2023-03-25'
      AND region IS NOT NULL
),
summary AS (
    -- Multi-level aggregation
    SELECT
        DATE_TRUNC('QUARTER', opened_date) AS quarter_start,
        PERCENTILE_APPROX(sqft_whole_2, 0.95) AS sqft_whole_2_p95,
        APPROX_PERCENTILE(sqft_whole, 0.5) AS sqft_whole_p50,
        COUNT(DISTINCT id) AS distinct_ids,
        SORT_ARRAY(COLLECT_SET(city)) AS city_list
    FROM base
    GROUP BY DATE_TRUNC('QUARTER', opened_date)
)
SELECT
    quarter_start,
    sqft_whole_2_p95,
    sqft_whole_p50,
    distinct_ids,
    city_list
FROM summary
ORDER BY sqft_whole_p50;
