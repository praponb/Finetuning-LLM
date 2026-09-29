SELECT city, rating_range
FROM (
    SELECT city, MAX(rating) - MIN(rating) AS rating_range
    FROM suppliers
    GROUP BY city
) AS agg
WHERE rating_range > 69
ORDER BY rating_range;
