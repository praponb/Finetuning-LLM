SELECT *
FROM (
    SELECT city, ROUND(AVG(rating), 2) AS avg_rating
    FROM suppliers
    GROUP BY city
) AS t
WHERE avg_rating < 48;
