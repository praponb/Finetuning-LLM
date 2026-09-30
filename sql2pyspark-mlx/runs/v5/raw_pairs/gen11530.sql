SELECT
    s.started_at,
    s.customer_id,
    s.browser,
    we.page,
    we.session_id,
    we.duration_sec
FROM sessions s
LEFT OUTER JOIN web_events we ON s.id = we.session_id
WHERE HOUR(s.started_at) BETWEEN 9 AND 16
