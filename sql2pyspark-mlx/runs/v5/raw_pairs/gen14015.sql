SELECT city, cnt
FROM (
    SELECT city, COUNT(*) AS cnt
    FROM stores
    GROUP BY city
) AS sub
WHERE cnt > 9
ORDER BY cnt DESC
