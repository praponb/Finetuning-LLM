WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CONCAT_WS(',', page, event_type) AS page_event_type_key,
        COALESCE(page, 'NONE') AS page_filled
    FROM web_events
    WHERE session_id IN (9, 40)
      AND event_time > TIMESTAMP '2023-05-22 21:00:00'
)
-- Attach sessions attributes
SELECT
    src.id,
    src.session_id,
    src.event_type,
    src.page,
    src.event_time,
    src.duration_sec,
    src.page_event_type_key,
    p.browser
FROM cleaned src
LEFT JOIN sessions p
    ON src.session_id = p.id;
