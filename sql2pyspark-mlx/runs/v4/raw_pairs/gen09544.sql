SELECT
    we.id,
    we.duration_sec,
    s.browser,
    s.device,
    s.country,
    DATE_FORMAT(we.event_time, 'yyyy-MM-dd') event_time_label
FROM web_events we
LEFT OUTER JOIN sessions s ON we.session_id = s.id
WHERE we.session_id IN (3, 10, 12, 36);
