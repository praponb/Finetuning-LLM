SELECT
    CASE WHEN rating >= 4.6 THEN 'PREMIUM' ELSE 'STANDARD' END AS rating_tier,
    COUNT(CASE WHEN city = 'Singapore' THEN 1 END) AS singapore_count,
    PERCENTILE_APPROX(rating, ARRAY(0.25, 0.5, 0.75)) AS rating_quartiles,
    SUM(rating) AS total_rating,
    SORT_ARRAY(COLLECT_SET(city)) AS city_list,
    PERCENTILE_APPROX(rating, ARRAY(0.25, 0.5, 0.75)) AS rating_quartiles_2
FROM suppliers
GROUP BY CASE WHEN rating >= 4.6 THEN 'PREMIUM' ELSE 'STANDARD' END
HAVING COUNT(CASE WHEN city = 'Singapore' THEN 1 END) > 1;
