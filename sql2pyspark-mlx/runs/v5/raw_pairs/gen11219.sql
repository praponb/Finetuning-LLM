SELECT
    src.id,
    src.session_id,
    src.event_type,
    src.page,
    src.duration_sec,
    p.device,
    p.country,
    p.browser
FROM web_events src
LEFT JOIN sessions p
    ON src.session_id = p.id;
