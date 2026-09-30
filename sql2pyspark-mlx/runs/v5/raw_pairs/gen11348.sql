WITH recent_suppliers AS (
    SELECT *
    FROM suppliers
    WHERE city NOT IN ('Tokyo', 'Bangkok') AND country = 'JP' AND rating BETWEEN 3.7 AND 4.2
),
agg AS (
    SELECT CASE WHEN rating >= 4.1 THEN 'high' WHEN rating >= 2.0 THEN 'mid' ELSE 'low' END AS rating_bucket, MIN(rating) AS min_rating
    FROM recent_suppliers
    GROUP BY CASE WHEN rating >= 4.1 THEN 'high' WHEN rating >= 2.0 THEN 'mid' ELSE 'low' END
)
SELECT *
FROM agg
ORDER BY rating_bucket
LIMIT 50;
