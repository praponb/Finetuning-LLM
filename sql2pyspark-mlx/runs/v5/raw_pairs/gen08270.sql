WITH latest_match AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.browser,
        x.web_event_duration_sec,
        x.web_event_page
    FROM sessions s
    LEFT JOIN LATERAL (
            SELECT c.duration_sec AS web_event_duration_sec, c.page AS web_event_page
            FROM web_events c
            WHERE c.session_id = s.id
              AND c.event_time <= s.started_at
            ORDER BY c.event_time DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
SELECT
    id,
    browser,
    web_event_duration_sec,
    web_event_page,
    CASE WHEN browser RLIKE '^(Chrome|Firefox)$' THEN 'CORE' WHEN browser RLIKE '^(Safari|Edge)$' THEN 'OTHER_ZONE' ELSE 'GROUP_A' END AS browser_zone
FROM latest_match;
