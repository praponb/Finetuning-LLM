WITH valid_suppliers AS (
    SELECT *
    FROM suppliers
    WHERE rating < 3.1
),
agg AS (
    SELECT city, SUM(CASE WHEN city = 'London' THEN 1 ELSE 0 END) AS london_count, COUNT(DISTINCT rating) AS unique_countrys
    FROM valid_suppliers
    GROUP BY city
)
SELECT city, london_count, unique_countrys
FROM agg
ORDER BY unique_countrys DESC, city
LIMIT 20;
