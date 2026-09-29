WITH latest_match AS (
    SELECT
        s.*,
        top1.event_time,
        top1.duration_sec
    FROM sessions s
    LEFT JOIN LATERAL (
            SELECT c.event_time, c.duration_sec
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.duration_sec DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
SELECT
    id,
    customer_id,
    device,
    browser,
    country,
    event_time,
    CASE WHEN browser RLIKE '^(Safari|Firefox)$' THEN 'EXTENDED' ELSE 'GROUP_A' END AS browser_zone
FROM latest_match
