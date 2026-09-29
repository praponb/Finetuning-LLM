WITH eligible AS (
    SELECT s.*
    FROM suppliers s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products ch
            WHERE ch.supplier_id = s.id
              AND ch.category = 'Grocery'
        )
)
SELECT
    country,
    city,
    CASE WHEN rating >= 4.0 THEN 'HIGH' ELSE 'LOW' END AS rating_tier,
    AVG(rating) FILTER (WHERE city = 'New York') AS new_york_avg_rating,
    MAX(rating) FILTER (WHERE city = 'Chiang Mai') AS chiang_mai_max_rating,
    COUNT(CASE WHEN country = 'TH' THEN 1 END) AS th_count
FROM eligible
GROUP BY ROLLUP (country, city, CASE WHEN rating >= 4.0 THEN 'HIGH' ELSE 'LOW' END)
HAVING th_count > 1
ORDER BY chiang_mai_max_rating DESC NULLS FIRST
