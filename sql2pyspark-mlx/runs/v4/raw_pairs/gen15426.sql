SELECT
    id,
    event_type,
    duration_sec,
    RANK() OVER (PARTITION BY event_type ORDER BY duration_sec DESC) AS rnk
FROM web_events
WHERE event_type = 'add_to_cart'
