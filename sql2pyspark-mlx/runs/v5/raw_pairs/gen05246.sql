SELECT *
FROM (
    SELECT city, ROUND(AVG(rating), 2) AS avg_rating, ROUND(STDDEV(rating), 2) AS stddev_rating
    FROM suppliers
    GROUP BY city
) AS t
WHERE avg_rating >= 71;
