SELECT
    country,
    SUM(CASE WHEN city = 'Bangkok' THEN 1 ELSE 0 END) AS bangkok_count,
    MIN(rating) AS min_rating,
    MAX(rating) AS max_rating
FROM suppliers
WHERE UPPER(TRIM(supplier_name)) = 'EUROGOODS GMBH'
GROUP BY country
HAVING SUM(CASE WHEN city = 'Bangkok' THEN 1 ELSE 0 END) < 46
ORDER BY max_rating DESC
