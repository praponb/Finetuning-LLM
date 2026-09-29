WITH Src AS (
    SELECT *
    FROM stores
    WHERE region = 'North'
),
Summary AS (
    SELECT region, ROUND(AVG(sqft), 2) AS avg_sqft, COUNT(CASE WHEN city = 'New York' THEN 1 END) AS new_york_count
    FROM Src
    GROUP BY region
)
SELECT region, avg_sqft, new_york_count
FROM Summary;
