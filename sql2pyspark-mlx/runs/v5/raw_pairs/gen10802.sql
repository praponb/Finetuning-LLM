SELECT page, AVG(duration_sec) AS avg_duration_sec
FROM web_events
WHERE event_type IN ('add_to_cart', 'click', 'search') AND event_type IN ('purchase', 'search', 'click')
GROUP BY page
ORDER BY page DESC;
