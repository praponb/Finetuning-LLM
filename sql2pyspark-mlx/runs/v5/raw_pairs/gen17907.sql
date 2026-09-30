WITH valid_stores AS (
    SELECT *
    FROM stores
    WHERE sqft <> 15772
),
city_summary AS (
    SELECT city, DATE_FORMAT(opened_date, 'yyyy-MM') AS opened_month, SUM(sqft) AS total_sqft
    FROM valid_stores
    GROUP BY city, DATE_FORMAT(opened_date, 'yyyy-MM')
)
SELECT *
FROM city_summary
ORDER BY opened_month
