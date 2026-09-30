SELECT *
FROM (
    SELECT city, AVG(sqft) AS avg_sqft
    FROM stores
    GROUP BY city
) AS sub
WHERE avg_sqft >= 14000
