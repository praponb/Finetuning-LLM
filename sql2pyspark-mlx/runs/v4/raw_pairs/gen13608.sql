WITH filtered_suppliers AS (
    SELECT *, CONCAT_WS(',', country, city) AS country_city_key
    FROM suppliers
    WHERE city IN ('Berlin', 'London', 'New York')
),
city_summary AS (
    SELECT city, ROUND(AVG(rating), 2) AS avg_rating
    FROM filtered_suppliers
    GROUP BY city
)
SELECT city, avg_rating
FROM city_summary
ORDER BY avg_rating DESC, city;
