WITH flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN started_at - LAG(started_at) OVER (PARTITION BY browser ORDER BY started_at ASC, id ASC) > INTERVAL '60' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM sessions
),
sessions_sessions AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        customer_id,
        device,
        browser,
        started_at,
        SUM(is_new_burst) OVER (PARTITION BY browser ORDER BY started_at ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
with_latest AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.device,
        s.browser,
        s.session_id,
        top1.event_time
    FROM sessions_sessions s
    JOIN LATERAL (
            SELECT c.event_time
            FROM web_events c
            WHERE c.session_id = s.id
              AND c.event_time <= s.started_at
            ORDER BY c.event_time DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
latest_match AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.duration_sec
    FROM with_latest s
    LEFT JOIN LATERAL (
            SELECT c.duration_sec
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.duration_sec DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY browser, device ORDER BY session_id DESC, id ASC) AS rn
        FROM latest_match
    ) ranked
    WHERE rn = 1
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY device ORDER BY event_time ASC, id ASC) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY device ORDER BY event_time ASC, id ASC) + INTERVAL 60 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM ranked
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY device ORDER BY event_time ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM with_gaps
)
-- Multi-level aggregation
SELECT
    CASE WHEN is_new_session >= 1 THEN 'LARGE' ELSE 'SMALL' END AS is_new_session_tier,
    CASE WHEN burst_id >= 7 THEN 'LARGE' ELSE 'SMALL' END AS burst_id_tier,
    burst_id,
    ROUND(AVG(burst_id), 2) AS avg_burst_id,
    MAX(burst_id) AS max_burst_id,
    AVG(is_new_session) FILTER (WHERE device = 'desktop') AS desktop_avg_is_new_session,
    (UNIX_TIMESTAMP(MAX(event_time)) - UNIX_TIMESTAMP(MIN(event_time))) / 60 AS duration_min,
    (UNIX_TIMESTAMP(MAX(event_time)) - UNIX_TIMESTAMP(MIN(event_time))) / 60 AS duration_min_2
FROM islands
GROUP BY GROUPING SETS ((CASE WHEN is_new_session >= 1 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN burst_id >= 7 THEN 'LARGE' ELSE 'SMALL' END, burst_id), (CASE WHEN is_new_session >= 1 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN burst_id >= 7 THEN 'LARGE' ELSE 'SMALL' END), ())
