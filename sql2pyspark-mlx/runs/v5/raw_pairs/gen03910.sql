WITH eligible AS (
    SELECT s.*
    FROM sessions s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events x
            WHERE x.session_id = s.id
              AND x.event_time < s.started_at
        )
)
SELECT
    browser,
    COUNT(*) FILTER (WHERE device = 'desktop') AS desktop_rows,
    (UNIX_TIMESTAMP(MAX(started_at)) - UNIX_TIMESTAMP(MIN(started_at))) / 60 AS span_minutes
FROM eligible
GROUP BY 1
ORDER BY span_minutes DESC
