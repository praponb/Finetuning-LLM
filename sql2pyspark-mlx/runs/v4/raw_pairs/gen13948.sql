WITH base AS (
    SELECT *
    FROM suppliers
    WHERE supplier_name LIKE '%Lio%'
),
Agg AS (
    SELECT city, ROUND(AVG(rating), 2) avg_rating
    FROM base
    GROUP BY city
)
SELECT *
FROM Agg
WHERE avg_rating >= 27
ORDER BY city DESC
LIMIT 5;
