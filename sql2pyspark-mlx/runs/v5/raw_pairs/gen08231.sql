SELECT city, rating_range
FROM (
    SELECT city, MAX(rating) - MIN(rating) AS rating_range
    FROM suppliers
    GROUP BY city
) AS x
WHERE rating_range >= 29
ORDER BY rating_range DESC;
