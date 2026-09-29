WITH ranked AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY rating, id) AS row_num
        FROM suppliers
    ) x
    WHERE row_num = 1
)
SELECT
    CASE WHEN rating >= 3.7 THEN 'HIGH' ELSE 'LOW' END AS rating_tier,
    ROUND(SUM(rating) / NULLIF(COUNT(CASE WHEN city = 'Singapore' THEN 1 END), 0), 2) AS rating_per_singapore,
    SUM(CASE WHEN country = 'UK' THEN rating ELSE 0 END) AS uk_rating
FROM ranked
GROUP BY CASE WHEN rating >= 3.7 THEN 'HIGH' ELSE 'LOW' END
HAVING ROUND(SUM(rating) / NULLIF(COUNT(CASE WHEN city = 'Singapore' THEN 1 END), 0), 2) < 0
ORDER BY rating_per_singapore DESC;
