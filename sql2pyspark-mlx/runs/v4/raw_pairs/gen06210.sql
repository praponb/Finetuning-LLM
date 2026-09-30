SELECT
    s.browser,
    s.started_at,
    s.country,
    we.event_time,
    we.page,
    we.event_type,
    COALESCE(we.page, 'unassigned') AS page_filled
FROM sessions s
LEFT OUTER JOIN web_events we ON s.id = we.session_id
ORDER BY browser;
