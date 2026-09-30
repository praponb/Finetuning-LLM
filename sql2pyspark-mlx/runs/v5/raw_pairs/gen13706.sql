WITH src AS (
    SELECT *, LOWER(city) AS city_lower
    FROM stores
    WHERE opened_date < '2025-09-08'
),
StoresStats AS (
    SELECT MONTH(opened_date) AS opened_month, city, MAX(sqft) AS max_sqft
    FROM src
    GROUP BY MONTH(opened_date), city
)
SELECT *
FROM StoresStats
WHERE max_sqft > 9200
ORDER BY max_sqft DESC
