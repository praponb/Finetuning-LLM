SELECT
    event_type,
    SUM(duration_sec) AS total_duration_sec,
    COUNT(*) AS cnt,
    RANK() OVER (ORDER BY SUM(duration_sec) DESC) AS duration_sec_rank
FROM web_events
WHERE event_type IN ('page_view', 'add_to_cart')
GROUP BY event_type
