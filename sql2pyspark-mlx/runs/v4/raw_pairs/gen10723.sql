WITH valid_suppliers AS (
    SELECT *, CONCAT(supplier_name, '/', country) AS supplier_name_country
    FROM suppliers
    WHERE country != 'DE' AND country = 'JP'
),
summary AS (
    SELECT CASE WHEN rating >= 4.3 THEN 'high' WHEN rating >= 2.0 THEN 'mid' ELSE 'low' END AS rating_bucket, COUNT(DISTINCT rating) AS unique_countrys, ROUND(STDDEV(rating), 2) AS stddev_rating
    FROM valid_suppliers
    GROUP BY CASE WHEN rating >= 4.3 THEN 'high' WHEN rating >= 2.0 THEN 'mid' ELSE 'low' END
)
SELECT rating_bucket, unique_countrys, stddev_rating
FROM summary
WHERE stddev_rating < 21
ORDER BY rating_bucket;
