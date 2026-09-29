WITH latest_match AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        latest.event_type,
        latest.web_event_page
    FROM sessions s
    LEFT JOIN LATERAL (
            SELECT c.event_type, c.page AS web_event_page
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.duration_sec DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
SELECT
    id,
    device,
    browser,
    started_at,
    event_type,
    web_event_page,
    CASE WHEN device RLIKE '^(desktop|tablet)$' THEN 'GROUP_B' ELSE 'PRIORITY' END AS device_zone
FROM latest_match
ORDER BY event_type DESC NULLS LAST, browser
