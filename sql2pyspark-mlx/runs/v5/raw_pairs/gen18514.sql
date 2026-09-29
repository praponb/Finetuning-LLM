SELECT city, chiang_mai_count
FROM (
    SELECT city, COUNT(CASE WHEN city = 'Chiang Mai' THEN 1 END) AS chiang_mai_count
    FROM suppliers
    GROUP BY city
) AS sub
WHERE chiang_mai_count > 4
ORDER BY city;
