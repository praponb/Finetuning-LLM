WITH valid_suppliers AS (
    SELECT *
    FROM suppliers
    WHERE supplier_name LIKE '%Global%' AND rating < 2.2
),
summary AS (
    SELECT city, SUM(CASE WHEN city = 'New York' THEN 1 ELSE 0 END) AS new_york_count, ROUND(STDDEV(rating), 2) AS stddev_rating
    FROM valid_suppliers
    GROUP BY city
)
SELECT city, stddev_rating
FROM summary
WHERE stddev_rating >= 65
