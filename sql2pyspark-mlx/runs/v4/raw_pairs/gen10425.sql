SELECT *
FROM (
    SELECT city, AVG(rating) AS avg_rating
    FROM suppliers
    GROUP BY city
) AS s
WHERE avg_rating > 3.1
ORDER BY 1 ASC, 2 ASC
LIMIT 20
