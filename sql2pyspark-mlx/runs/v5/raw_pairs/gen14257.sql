WITH valid_suppliers AS (
    SELECT *, TRIM(city) AS clean_city
    FROM suppliers
    WHERE city = 'Tokyo'
),
suppliers_stats AS (
    SELECT city, ROUND(STDDEV(rating), 2) AS stddev_rating, MAX(rating) AS max_rating
    FROM valid_suppliers
    GROUP BY city
)
SELECT city, stddev_rating, max_rating
FROM suppliers_stats
WHERE stddev_rating >= 25
ORDER BY stddev_rating;
