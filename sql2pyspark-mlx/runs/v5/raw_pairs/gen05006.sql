WITH src AS (
    SELECT *, CASE WHEN city = 'Tokyo' THEN 1 ELSE 0 END AS is_tokyo
    FROM suppliers
    WHERE supplier_name LIKE 'Pac%'
),
suppliers_stats AS (
    SELECT city, ROUND((100 * SUM(CASE WHEN city = 'London' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_london, COUNT(is_tokyo) AS is_tokyo_count
    FROM src
    GROUP BY city
)
SELECT *
FROM suppliers_stats
ORDER BY pct_london, city
LIMIT 50;
