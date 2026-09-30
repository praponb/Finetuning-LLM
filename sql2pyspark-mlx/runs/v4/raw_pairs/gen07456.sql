WITH Src AS (
    SELECT *, REPLACE(supplier_name, ' ', '') AS supplier_name_slug
    FROM suppliers
    WHERE city IN ('Chiang Mai', 'Bangkok') AND (LENGTH(supplier_name) >= 6 OR city = 'Berlin')
),
city_summary AS (
    SELECT city, CASE WHEN country IN ('UK', 'JP') THEN 'core' ELSE 'other' END AS country_group, ROUND(STDDEV(rating), 2) AS stddev_rating
    FROM Src
    GROUP BY city, CASE WHEN country IN ('UK', 'JP') THEN 'core' ELSE 'other' END
)
SELECT city, country_group, stddev_rating
FROM city_summary
WHERE stddev_rating >= 59
ORDER BY country_group DESC;
