WITH ValidStores AS (
    SELECT *
    FROM stores
    WHERE NOT (region = 'East' AND city != 'Singapore')
),
city_summary AS (
    SELECT city, COUNT(*) AS total_count
    FROM ValidStores
    GROUP BY city
)
SELECT city, total_count
FROM city_summary
WHERE total_count > 8
