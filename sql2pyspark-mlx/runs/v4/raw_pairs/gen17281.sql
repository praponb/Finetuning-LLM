WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT *, CONCAT(page, ', ', event_type) AS page_event_type
    FROM web_events
    WHERE event_type IN ('purchase', 'click')
)
SELECT id, event_type
FROM filtered
INTERSECT ALL
SELECT id, event_type
FROM web_events
WHERE session_id IN (8, 9, 35)
