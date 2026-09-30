SELECT *
FROM (
    SELECT country, MIN(rating) AS min_rating, SUM(rating) AS total_rating
    FROM suppliers
    GROUP BY country
) AS x
WHERE total_rating > 9
ORDER BY 2, total_rating
