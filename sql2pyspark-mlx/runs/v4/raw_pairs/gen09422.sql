SELECT
    we.id,
    we.event_time,
    we.duration_sec,
    we.session_id,
    s.customer_id,
    TRIM(s.browser) AS clean_browser
FROM web_events we
JOIN sessions s ON we.session_id = s.id
WHERE s.customer_id IN (8, 12, 13)
ORDER BY customer_id DESC
