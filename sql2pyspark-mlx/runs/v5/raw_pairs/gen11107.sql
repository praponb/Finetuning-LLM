WITH RecentSuppliers AS (
    SELECT *
    FROM suppliers
    WHERE rating < 4.2 OR rating < 2.2
),
summary AS (
    SELECT city, MIN(rating) AS min_rating
    FROM RecentSuppliers
    GROUP BY city
)
SELECT city, min_rating
FROM summary
ORDER BY min_rating DESC, city
LIMIT 10;
