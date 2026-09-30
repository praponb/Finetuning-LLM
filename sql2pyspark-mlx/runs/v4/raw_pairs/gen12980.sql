WITH ranked AS (
    SELECT
        id,
        event_type,
        event_time,
        ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY event_time DESC, id) AS rnk
    FROM web_events
    WHERE event_type IN ('purchase', 'click', 'add_to_cart')
)
SELECT id, event_type, event_time
FROM ranked
WHERE rnk = 1
ORDER BY event_type;
