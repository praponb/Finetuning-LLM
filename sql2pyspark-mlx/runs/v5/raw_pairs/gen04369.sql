WITH with_prev AS (
    SELECT *, LAG(started_at) OVER (PARTITION BY country ORDER BY started_at, id) AS prev_started_at
    FROM sessions
),
gaps AS (
    SELECT *, CASE WHEN prev_started_at IS NULL OR started_at > prev_started_at + INTERVAL '14' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM with_prev
),
islands AS (
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY country ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM gaps
),
kept AS (
    SELECT s.*
    FROM islands s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events c
            WHERE c.session_id = s.id
              AND c.event_time <= s.started_at
        )
),
kept_sessions AS (
    SELECT s.*
    FROM kept s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events x
            WHERE x.session_id = s.id
              AND x.event_time >= s.prev_started_at
        )
),
kept_sessions_2 AS (
    SELECT s.*
    FROM kept_sessions s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events c
            WHERE c.session_id = s.id
              AND c.event_time >= s.prev_started_at
        )
)
SELECT
    c.segment,
    a.device,
    COUNT(*) AS num_rows,
    ROUND(SUM(a.session_start_flag), 2) AS total_session_start_flag,
    ROUND(SUM(a.session_id), 2) AS total_session_id,
    GROUPING_ID() AS gid
FROM kept_sessions_2 a
JOIN customers c ON a.customer_id = c.id
GROUP BY CUBE (c.segment, a.device)
ORDER BY segment DESC NULLS FIRST
