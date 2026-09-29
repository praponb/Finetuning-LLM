SELECT
    CASE WHEN rating >= 4.2 THEN 'high' WHEN rating >= 1.5 THEN 'mid' ELSE 'low' END AS rating_bucket,
    city,
    SUM(rating) AS total_rating,
    MIN(rating) AS min_rating,
    SORT_ARRAY(COLLECT_SET(city)) AS city_list
FROM suppliers
GROUP BY CASE WHEN rating >= 4.2 THEN 'high' WHEN rating >= 1.5 THEN 'mid' ELSE 'low' END, city;
