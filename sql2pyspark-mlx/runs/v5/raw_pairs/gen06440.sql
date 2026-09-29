WITH src AS (
    SELECT *
    FROM suppliers
    WHERE LENGTH(supplier_name) >= 10
),
suppliers_stats AS (
    SELECT city, SUM(CASE WHEN city = 'Bangkok' THEN 1 ELSE 0 END) AS bangkok_count, COUNT(rating) AS rating_count
    FROM src
    GROUP BY city
)
SELECT city, bangkok_count, rating_count
FROM suppliers_stats;
