SELECT
    s.id,
    s.customer_id,
    s.device,
    s.browser,
    s.started_at,
    s.country,
    top1.page,
    top1.event_type
FROM sessions s
JOIN LATERAL (
        SELECT c.page, c.event_type
        FROM web_events c
        WHERE c.session_id = s.id
        ORDER BY c.duration_sec DESC, c.id DESC
        LIMIT 1
    ) top1 ON TRUE
