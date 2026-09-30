SELECT *
FROM (
    SELECT is_active, COUNT(DISTINCT age) AS unique_countrys
    FROM customers
    WHERE email LIKE '%@startup.io'
    GROUP BY is_active
) AS x
WHERE unique_countrys >= 7
