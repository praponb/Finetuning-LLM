SELECT
    web_events.session_id,
    web_events.event_type,
    sessions.device,
    sessions.browser,
    sessions.country,
    CONCAT_WS('-', web_events.event_type, web_events.page) AS event_type_page_key
FROM web_events
INNER JOIN sessions ON web_events.session_id = sessions.id
WHERE sessions.started_at < '2023-04-01' AND web_events.event_type = 'add_to_cart'
