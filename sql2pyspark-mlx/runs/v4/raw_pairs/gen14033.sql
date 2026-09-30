WITH with_latest AS (
    SELECT s.*, top1.web_event_page
    FROM sessions s
    LEFT JOIN LATERAL (
            SELECT c.page AS web_event_page
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.event_time DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
SELECT
    id,
    device,
    browser,
    started_at,
    country
FROM with_latest
ORDER BY started_at
