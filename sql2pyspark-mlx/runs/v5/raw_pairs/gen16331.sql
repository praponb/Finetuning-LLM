WITH cleaned AS (
    SELECT
        id,
        customer_id,
        browser,
        CAST(started_at AS DATE) AS started_at_day,
        LENGTH(device) AS device_length,
        COALESCE(browser, 'N/A') AS resolved_browser
    FROM sessions
    WHERE browser IS DISTINCT FROM 'Chrome'
),
with_dims AS (
    SELECT
        e.id,
        e.customer_id,
        e.browser,
        e.started_at_day,
        e.device_length,
        e.resolved_browser,
        b.country,
        b.credit_limit
    FROM cleaned e
    INNER JOIN customers b
        ON e.customer_id = b.id
    WHERE b.email LIKE '%@startup.io'
),
latest_match AS (
    SELECT
        s.id,
        s.device_length,
        s.country,
        top1.web_event_duration_sec
    FROM with_dims s
    LEFT JOIN LATERAL (
            SELECT c.duration_sec AS web_event_duration_sec
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.event_time DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
kept AS (
    SELECT s.*
    FROM latest_match s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events ch
            WHERE ch.session_id = s.id
              AND ch.session_id IN (14, 27, 31, 35)
        )
)
SELECT
    id,
    country,
    web_event_duration_sec,
    CASE WHEN country RLIKE '^(JP|DE)$' THEN 'PRIORITY' WHEN country RLIKE '^(TH|US)$' THEN 'GROUP_B' ELSE 'OTHER_ZONE' END AS country_zone
FROM kept;
