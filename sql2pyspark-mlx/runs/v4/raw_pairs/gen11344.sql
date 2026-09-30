WITH Base AS (
    SELECT *
    FROM suppliers
    WHERE city IS NULL
),
agg AS (
    SELECT country, MAX(rating) AS max_rating
    FROM Base
    GROUP BY country
)
SELECT country, max_rating
FROM agg
WHERE max_rating >= 3.4
ORDER BY max_rating DESC
