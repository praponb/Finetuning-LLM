WITH src AS (
    SELECT *
    FROM stores
    WHERE NOT (store_name LIKE 'Mal%' AND (sqft IS NOT NULL OR region = 'East'))
),
summary AS (
    SELECT region, AVG(sqft) avg_sqft, MIN(opened_date) first_opened_date
    FROM src
    GROUP BY region
)
SELECT *
FROM summary
WHERE avg_sqft < 15800
ORDER BY first_opened_date ASC
