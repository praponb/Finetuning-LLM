SELECT
    id,
    country,
    rating,
    AVG(rating) OVER (PARTITION BY country) AS country_avg_rating
FROM suppliers
WHERE UPPER(TRIM(supplier_name)) = 'LION CITY IMPORTS'
ORDER BY 1 DESC
