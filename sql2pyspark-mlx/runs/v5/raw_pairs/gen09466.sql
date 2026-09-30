SELECT
    CASE WHEN rating >= 4.3 THEN 'LARGE' ELSE 'SMALL' END AS rating_tier,
    AVG(rating) FILTER (WHERE city = 'London') AS london_avg_rating,
    PERCENTILE_APPROX(rating, ARRAY(0.25, 0.5, 0.75)) AS rating_quartiles,
    COUNT(*) AS event_count,
    PERCENTILE_APPROX(rating, ARRAY(0.25, 0.5, 0.75)) AS rating_quartiles_2
FROM suppliers
GROUP BY CASE WHEN rating >= 4.3 THEN 'LARGE' ELSE 'SMALL' END
HAVING london_avg_rating > 0
ORDER BY london_avg_rating DESC NULLS LAST;
