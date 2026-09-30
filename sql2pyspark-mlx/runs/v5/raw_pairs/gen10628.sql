SELECT
    we.id,
    we.page,
    we.duration_sec,
    we.event_time,
    s.device,
    s.browser,
    s.country
FROM web_events we
FULL OUTER JOIN sessions s ON s.id = we.session_id
WHERE we.duration_sec >= 149 AND s.country <> 'UK';
