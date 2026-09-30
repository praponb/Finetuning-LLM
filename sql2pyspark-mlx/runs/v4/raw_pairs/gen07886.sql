SELECT
    CASE WHEN rating >= 4.5 THEN 'HIGH' ELSE 'LOW' END AS rating_tier,
    country,
    COUNT_IF(country = 'UK') AS uk_events,
    COUNT(*) FILTER (WHERE country = 'JP') AS jp_rows,
    COUNT_IF(city = 'Tokyo') AS tokyo_events,
    AVG(rating) FILTER (WHERE country = 'SG') AS sg_avg_rating,
    COUNT(*) FILTER (WHERE country = 'JP') AS jp_rows_2
FROM suppliers
GROUP BY 1, 2
HAVING sg_avg_rating < 0
