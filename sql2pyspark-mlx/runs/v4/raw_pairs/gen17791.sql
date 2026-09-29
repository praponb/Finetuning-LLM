SELECT
    CASE WHEN rating >= 4.5 THEN 'HIGH' ELSE 'LOW' END AS rating_tier,
    country,
    COUNT_IF(city = 'Tokyo') AS tokyo_events,
    COUNT(*) AS event_count,
    MAX(rating) FILTER (WHERE city = 'New York') AS new_york_max_rating
FROM suppliers
GROUP BY 1, 2
