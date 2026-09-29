WITH latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM stores
    QUALIFY RANK() OVER (PARTITION BY city, region ORDER BY opened_date) = 1
),
latest_stores AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, RANK() OVER (PARTITION BY region ORDER BY sqft DESC NULLS FIRST) AS dedup_rank
    FROM latest
    QUALIFY dedup_rank = 1
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        TRIM(city) AS clean_city,
        LENGTH(city) AS city_length,
        COALESCE(region, 'OTHER') AS region_clean
    FROM latest_stores
    WHERE opened_date >= TO_DATE('2023-03-29')
      AND region IS NOT NULL
      AND sqft = 6036
)
SELECT
    id,
    city,
    opened_date,
    sqft,
    clean_city,
    CASE WHEN city RLIKE '^(Singapore|Chiang Mai)$' THEN 'GROUP_B' ELSE 'OTHER_ZONE' END AS city_zone
FROM valid
ORDER BY sqft NULLS FIRST, id DESC
LIMIT 5
