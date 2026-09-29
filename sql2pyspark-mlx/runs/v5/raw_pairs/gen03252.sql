SELECT
    country,
    COUNT(*) AS cnt,
    ROUND(AVG(rating), 2) AS avg_rating,
    SORT_ARRAY(COLLECT_SET(city)) AS city_list
FROM suppliers
WHERE supplier_name LIKE '%Siam%' AND country IN ('DE', 'TH', 'SG')
GROUP BY country
