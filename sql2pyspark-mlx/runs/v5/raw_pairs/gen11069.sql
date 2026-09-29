WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        session_id,
        event_type,
        page,
        event_time,
        duration_sec,
        TRIM(event_type) AS clean_event_type
    FROM web_events
    WHERE event_time >= DATE '2023-03-12'
      AND page IS NOT DISTINCT FROM '/search'
),
prev_events AS (
    -- Previous event time per key
    SELECT *, LAG(event_time) OVER (PARTITION BY event_type ORDER BY event_time, id) AS prev_event_time
    FROM base
),
gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_event_time IS NULL OR event_time > prev_event_time + INTERVAL 30 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM prev_events
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        session_id,
        event_type,
        page,
        event_time,
        duration_sec,
        SUM(session_start_flag) OVER (PARTITION BY event_type ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM gaps
)
-- Multi-level aggregation
SELECT
    session_no,
    event_type,
    STDDEV_SAMP(duration_sec) AS duration_sec_stddev,
    COUNT(CASE WHEN page = '/checkout' THEN 1 END) AS checkout_count,
    ROUND(SUM(duration_sec) / NULLIF(COUNT(CASE WHEN event_type = 'search' THEN 1 END), 0), 2) AS duration_sec_per_search,
    GROUPING(session_no) AS grouping_level
FROM sessionized
GROUP BY GROUPING SETS ((session_no, event_type), (session_no), ())
ORDER BY checkout_count DESC NULLS FIRST
