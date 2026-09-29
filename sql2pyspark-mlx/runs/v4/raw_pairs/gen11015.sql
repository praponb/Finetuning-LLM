WITH base AS (
    -- Basic cleansing and defaults
    SELECT *, LOWER(city) AS city_lower
    FROM stores
    WHERE opened_date > DATE '2023-04-01'
      AND region IS NOT NULL
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        city,
        region,
        sqft
    FROM base
) src
PIVOT (
    AVG(sqft) AS total,
    COUNT(1) AS cnt
    FOR region IN ('South', 'West')
);
