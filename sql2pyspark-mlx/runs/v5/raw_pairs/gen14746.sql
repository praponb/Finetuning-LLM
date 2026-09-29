WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        device,
        browser,
        started_at,
        country,
        TRIM(country) clean_country
    FROM sessions
    WHERE started_at >= TO_DATE('2023-04-05')
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM base s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events c
            WHERE c.session_id = s.id
              AND c.event_time >= s.started_at
        )
),
eligible_sessions AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM eligible s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events c
            WHERE c.session_id = s.id
              AND c.session_id = 22
        )
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM eligible_sessions
    QUALIFY ROW_NUMBER() OVER (PARTITION BY device ORDER BY started_at DESC NULLS FIRST, id) = 1
)
-- Aggregate with conditional metrics
SELECT
    browser,
    COUNT_IF(device = 'tablet') AS tablet_events,
    COUNT(*) FILTER (WHERE browser = 'Firefox') AS firefox_rows,
    (UNIX_TIMESTAMP(MAX(started_at)) - UNIX_TIMESTAMP(MIN(started_at))) / 60 AS duration_min
FROM deduped
GROUP BY browser
ORDER BY tablet_events NULLS FIRST;
