SELECT
    country,
    COUNT(rating) AS rating_count,
    MAX(rating) AS max_rating,
    SORT_ARRAY(COLLECT_SET(country)) AS country_list
FROM suppliers
WHERE supplier_name LIKE '%Siam%'
GROUP BY country;
