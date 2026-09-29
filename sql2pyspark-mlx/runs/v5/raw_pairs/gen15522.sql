WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        store_name,
        city,
        opened_date,
        sqft,
        LOWER(region) AS region_lower,
        DATEDIFF(CURRENT_DATE(), opened_date) AS days_since_opened_date
    FROM stores
    WHERE region IS NOT NULL
),
cleaned_stores AS (
    -- Keep recent, valid rows
    SELECT *, sqft + days_since_opened_date AS sqft_plus_days_since_opened_date
    FROM cleaned
    WHERE sqft <> 12453
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM cleaned_stores
    QUALIFY ROW_NUMBER() OVER (PARTITION BY city ORDER BY sqft NULLS LAST, id) = 1
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        city,
        sqft,
        region_lower,
        days_since_opened_date,
        days_since_opened_date + sqft_plus_days_since_opened_date AS days_since_opened_date_plus_sqft_plus_days_since_opened_date,
        CASE WHEN sqft >= 16057 THEN 'High' WHEN sqft >= 3920 THEN 'Medium' ELSE 'Low' END AS sqft_level
    FROM deduped
    WHERE opened_date > DATE '2023-04-14'
      AND opened_date > '2024-11-01'
)
-- Grouped metrics
SELECT
    CASE WHEN days_since_opened_date >= 80 THEN 'PREMIUM' ELSE 'STANDARD' END AS days_since_opened_date_tier,
    CASE WHEN sqft >= 14751 THEN 'LARGE' ELSE 'SMALL' END AS sqft_tier,
    ROUND(SUM(days_since_opened_date_plus_sqft_plus_days_since_opened_date) / NULLIF(COUNT(CASE WHEN city = 'Chiang Mai' THEN 1 END), 0), 2) AS days_since_opened_date_plus_sqft_plus_days_since_opened_date_per_chiang_mai,
    PERCENTILE_APPROX(sqft, 0.95) AS sqft_p95,
    COUNT(CASE WHEN city = 'Bangkok' THEN 1 END) AS bangkok_count,
    COUNT_IF(city = 'Bangkok') AS bangkok_events,
    MAX(days_since_opened_date) FILTER (WHERE city = 'New York') AS new_york_max_days_since_opened_date
FROM valid
GROUP BY CASE WHEN days_since_opened_date >= 80 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN sqft >= 14751 THEN 'LARGE' ELSE 'SMALL' END
