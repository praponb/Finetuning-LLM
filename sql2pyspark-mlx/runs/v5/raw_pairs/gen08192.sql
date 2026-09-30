WITH filtered_stores AS (
    SELECT *, INITCAP(TRIM(city)) AS city_title
    FROM stores
    WHERE region = 'West' AND region IN ('West', 'Central')
),
CitySummary AS (
    SELECT city, MIN(sqft) AS min_sqft
    FROM filtered_stores
    GROUP BY city
)
SELECT city, min_sqft
FROM CitySummary
WHERE min_sqft < 13300;
