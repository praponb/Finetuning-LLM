WITH flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(started_at) OVER (PARTITION BY country ORDER BY started_at ASC, id ASC) IS NULL
                 OR started_at > LAG(started_at) OVER (PARTITION BY country ORDER BY started_at ASC, id ASC) + INTERVAL 14 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM sessions
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY country ORDER BY started_at ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
with_last AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        x.web_event_event_time,
        x.duration_sec
    FROM sessionized s
    LEFT JOIN LATERAL (
            SELECT c.event_time AS web_event_event_time, c.duration_sec
            FROM web_events c
            WHERE c.session_id = s.id
              AND c.event_time <= s.started_at
            ORDER BY c.event_time DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
latest AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        started_at,
        session_start_flag,
        session_id,
        web_event_event_time,
        duration_sec
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY country, browser ORDER BY duration_sec DESC, id ASC) AS latest_rank
        FROM with_last
    ) t
    WHERE latest_rank = 1
),
with_last_sessions AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.session_start_flag,
        s.web_event_event_time,
        x.web_event_event_type,
        x.web_event_page
    FROM latest s
    LEFT JOIN LATERAL (
            SELECT c.event_type AS web_event_event_type, c.page AS web_event_page
            FROM web_events c
            WHERE c.session_id = s.id
              AND c.event_time <= s.started_at
            ORDER BY c.event_time DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
-- Grouped metrics
SELECT
    web_event_event_type,
    DATE_TRUNC('YEAR', web_event_event_time) AS year_start,
    CASE WHEN session_start_flag >= 1 THEN 'HIGH' ELSE 'LOW' END AS session_start_flag_tier,
    COUNT(*) AS cnt,
    SORT_ARRAY(COLLECT_SET(web_event_event_type)) AS web_event_event_type_list,
    (UNIX_TIMESTAMP(MAX(web_event_event_time)) - UNIX_TIMESTAMP(MIN(web_event_event_time))) / 60 AS duration_min,
    COUNT(*) AS event_count,
    MAX(web_event_event_time) AS last_web_event_event_time,
    COUNT(CASE WHEN web_event_page = '/cart' THEN 1 END) AS cart_count
FROM with_last_sessions
GROUP BY GROUPING SETS ((web_event_event_type, DATE_TRUNC('YEAR', web_event_event_time), CASE WHEN session_start_flag >= 1 THEN 'HIGH' ELSE 'LOW' END), (web_event_event_type, DATE_TRUNC('YEAR', web_event_event_time)), (web_event_event_type))
HAVING COUNT(CASE WHEN web_event_page = '/cart' THEN 1 END) < 1000
ORDER BY web_event_event_type ASC;
