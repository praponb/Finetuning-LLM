SELECT *
FROM (
    SELECT city, ROUND(AVG(sqft), 2) AS avg_sqft
    FROM stores
    WHERE region IS NOT NULL
    GROUP BY city
) AS s
WHERE avg_sqft >= 67
ORDER BY avg_sqft;
