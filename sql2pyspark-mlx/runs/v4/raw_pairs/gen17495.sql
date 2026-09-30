WITH ranked_web_events AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY page ORDER BY event_time DESC, id) AS rn
    FROM web_events
)
SELECT
    id,
    page,
    event_time,
    event_type
FROM ranked_web_events
WHERE rn <= 5
ORDER BY page;
