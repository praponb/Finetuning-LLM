WITH latest_match AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.device,
        s.browser,
        x.web_event_event_time
    FROM sessions s
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
    FROM latest_match s
    WHERE EXISTS (
            SELECT 1
            FROM web_events x
            WHERE x.session_id = s.id
              AND x.event_time >= s.web_event_event_time
        )
),
kept_sessions AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM kept s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events ch
            WHERE ch.session_id = s.id
              AND ch.event_time < s.web_event_event_time
        )
),
enriched AS (
    -- Running and moving aggregates
    SELECT *, ROW_NUMBER() OVER (PARTITION BY device ORDER BY web_event_event_time DESC, id) AS row_seq
    FROM kept_sessions
)
SELECT
    id,
    device,
    browser,
    row_seq,
    CASE WHEN device RLIKE '^(mobile|tablet)$' THEN 'CORE' ELSE 'PRIORITY' END AS device_zone
FROM enriched
ORDER BY row_seq DESC NULLS FIRST;
