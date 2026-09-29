WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, SUBSTRING(device, 1, 3) AS device_prefix
    FROM sessions
    WHERE started_at >= TIMESTAMP '2023-02-16 03:00:00'
),
prev_events AS (
    -- Previous event time per key
    SELECT *, LAG(started_at) OVER (PARTITION BY browser ORDER BY started_at, id) AS prev_started_at
    FROM cleaned
),
flagged AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_started_at IS NULL OR started_at > prev_started_at + INTERVAL 90 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM prev_events
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY browser ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
with_last AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.customer_id,
        s.browser,
        s.started_at,
        top1.event_time
    FROM islands s
    LEFT JOIN LATERAL (
            SELECT c.event_time
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.duration_sec DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
with_latest AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.browser,
        s.event_time,
        x.web_event_event_time
    FROM with_last s
    LEFT JOIN LATERAL (
            SELECT c.event_time AS web_event_event_time
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.event_time DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM with_latest s
    WHERE EXISTS (
            SELECT 1
            FROM web_events x
            WHERE x.session_id = s.id
              AND x.session_id IN (2, 17, 30, 36)
        )
)
SELECT
    id,
    browser,
    CASE WHEN browser RLIKE '^(Safari|Firefox)$' THEN 'PRIORITY' ELSE 'CORE' END AS browser_zone
FROM kept
ORDER BY browser;
