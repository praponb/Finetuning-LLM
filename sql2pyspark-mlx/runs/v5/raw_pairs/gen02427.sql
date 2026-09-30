WITH src AS (
    SELECT *
    FROM suppliers
    WHERE country <> 'SG'
),
summary AS (
    SELECT city, CASE WHEN rating >= 4.6 THEN 'high' WHEN rating >= 1.5 THEN 'mid' ELSE 'low' END AS rating_bucket, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(city))) AS city_values
    FROM src
    GROUP BY city, CASE WHEN rating >= 4.6 THEN 'high' WHEN rating >= 1.5 THEN 'mid' ELSE 'low' END
)
SELECT city, rating_bucket, city_values
FROM summary
ORDER BY rating_bucket, city
LIMIT 20
