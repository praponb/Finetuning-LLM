SELECT
    id,
    event_type,
    CONCAT(event_type, ' - ', page) AS event_type_page,
    CONCAT_WS('-', page, event_type) AS page_event_type_key
FROM web_events
WHERE event_type = 'click' AND session_id = 26 AND event_type IN ('purchase', 'search')
ORDER BY id DESC, event_type;
