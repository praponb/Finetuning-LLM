SELECT country, max_rating
FROM (
    SELECT country, MAX(rating) AS max_rating
    FROM suppliers
    WHERE country = 'US'
    GROUP BY country
) AS x
WHERE max_rating >= 3.0
ORDER BY country
