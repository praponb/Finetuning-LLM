SELECT
    CASE WHEN rating >= 4.3 THEN 'PREMIUM' ELSE 'STANDARD' END AS rating_tier,
    ROUND(SUM(rating) / NULLIF(COUNT(CASE WHEN city = 'New York' THEN 1 END), 0), 2) AS rating_per_new_york,
    SIZE(COLLECT_SET(country)) AS num_countries,
    COUNT(*) AS event_count,
    MIN(rating) AS min_rating,
    SIZE(COLLECT_SET(city)) AS num_cities
FROM suppliers
GROUP BY rating_tier
HAVING MIN(rating) > 0;
