WITH Src AS (
    SELECT *, CONCAT(supplier_name, '/', country) AS supplier_name_country
    FROM suppliers
    WHERE supplier_name IS NOT NULL AND (rating > 2.5 OR country = 'DE')
),
suppliers_stats AS (
    SELECT CASE WHEN rating >= 4.4 THEN 'high' WHEN rating >= 2.0 THEN 'mid' ELSE 'low' END AS rating_bucket, SUM(rating) AS total_rating
    FROM Src
    GROUP BY CASE WHEN rating >= 4.4 THEN 'high' WHEN rating >= 2.0 THEN 'mid' ELSE 'low' END
)
SELECT *
FROM suppliers_stats
WHERE total_rating < 20
