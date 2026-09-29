SELECT *
FROM (
    SELECT country, COUNT(DISTINCT rating) AS unique_countrys
    FROM suppliers
    GROUP BY country
) AS agg
WHERE unique_countrys >= 10
ORDER BY country, unique_countrys;
