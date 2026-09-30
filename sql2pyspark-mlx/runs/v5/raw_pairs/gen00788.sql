WITH not_flagged AS (
    SELECT s.*
    FROM sessions s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events x
            WHERE x.session_id = s.id
              AND x.session_id IN (17, 31)
        )
),
with_last AS (
    SELECT
        s.*,
        x.web_event_event_time,
        x.event_type
    FROM not_flagged s
    JOIN LATERAL (
            SELECT c.event_time AS web_event_event_time, c.event_type
            FROM web_events c
            WHERE c.session_id = s.id
              AND c.event_time <= s.started_at
            ORDER BY c.event_time DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
kept AS (
    SELECT s.*
    FROM with_last s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events c
            WHERE c.session_id = s.id
              AND c.event_time < s.web_event_event_time
        )
),
kept_sessions AS (
    SELECT s.*
    FROM kept s
    WHERE EXISTS (
            SELECT 1
            FROM web_events c
            WHERE c.session_id = s.id
              AND c.event_type = 'purchase'
        )
)
SELECT
    s.id,
    s.customer_id,
    s.browser,
    s.started_at,
    s.country,
    s.web_event_event_time,
    CASE WHEN browser RLIKE '^(Firefox|Safari)$' THEN 'PRIORITY' WHEN browser RLIKE '^(Chrome|Edge)$' THEN 'EXTENDED' ELSE 'GROUP_B' END AS browser_zone,
    (SELECT MAX(x.discount) FROM orders x WHERE x.customer_id = s.customer_id) AS max_order_discount
FROM kept_sessions s
