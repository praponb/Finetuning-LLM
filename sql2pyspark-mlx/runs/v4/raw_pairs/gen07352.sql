WITH with_latest AS (
    SELECT
        s.*,
        latest.event_type,
        latest.web_event_page
    FROM sessions s
    LEFT JOIN LATERAL (
            SELECT c.event_type, c.page AS web_event_page
            FROM web_events c
            WHERE c.session_id = s.id
              AND c.event_time <= s.started_at
            ORDER BY c.event_time DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
SELECT
    DATE_TRUNC('WEEK', started_at) AS week_start,
    customer_id,
    COUNT(*) FILTER (WHERE country = 'UK') AS uk_rows,
    COUNT(*) AS cnt,
    SIZE(COLLECT_SET(device)) AS num_devices,
    COUNT(*) FILTER (WHERE browser = 'Safari') AS safari_rows
FROM with_latest
GROUP BY DATE_TRUNC('WEEK', started_at), customer_id
HAVING safari_rows > 1
ORDER BY customer_id DESC NULLS LAST;
