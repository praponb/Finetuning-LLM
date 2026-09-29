SELECT
    s.country,
    s.customer_id,
    s.device,
    we.event_time,
    we.page,
    we.event_type,
    LENGTH(we.page) AS page_length
FROM sessions s
LEFT JOIN web_events we ON s.id = we.session_id
WHERE s.device <> 'desktop'
