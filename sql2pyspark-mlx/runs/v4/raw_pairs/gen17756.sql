SELECT
    country,
    city,
    COUNT(CASE WHEN city = 'Singapore' THEN 1 END) AS singapore_count,
    ROUND(STDDEV(rating), 2) AS stddev_rating,
    COUNT(DISTINCT rating) AS unique_citys
FROM suppliers
WHERE country = 'UK' AND LOWER(supplier_name) LIKE '%uro%'
GROUP BY country, city
HAVING unique_citys < 4
ORDER BY unique_citys DESC, country
