WITH valid_suppliers AS (
    SELECT *
    FROM suppliers
    WHERE rating > 2.7
),
suppliers_stats AS (
    SELECT CASE WHEN rating >= 4.1 THEN 'high' WHEN rating >= 2.1 THEN 'mid' ELSE 'low' END AS rating_bucket, country, SUM(CASE WHEN country = 'JP' THEN rating ELSE 0 END) AS jp_rating
    FROM valid_suppliers
    GROUP BY CASE WHEN rating >= 4.1 THEN 'high' WHEN rating >= 2.1 THEN 'mid' ELSE 'low' END, country
)
SELECT *
FROM suppliers_stats
WHERE jp_rating >= 28
ORDER BY country DESC;
