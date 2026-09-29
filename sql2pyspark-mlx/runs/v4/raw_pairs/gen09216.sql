WITH base AS (
    SELECT
        *,
        HOUR(started_at) AS started_at_hour,
        COALESCE(browser, 'OTHER') AS browser_clean
    FROM sessions
    WHERE device IN ('tablet', 'mobile')
)
SELECT s.*
FROM base s
WHERE NOT EXISTS (
        SELECT 1
        FROM web_events x
        WHERE x.session_id = s.id
          AND x.event_time <= s.started_at
    );
