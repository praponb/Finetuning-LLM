WITH flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY event_type ORDER BY event_time, id) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY event_type ORDER BY event_time, id) + INTERVAL 90 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM web_events
),
sessions_web_events AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        session_id,
        event_type,
        page,
        event_time,
        duration_sec,
        SUM(session_start_flag) OVER (PARTITION BY event_type ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id_2
    FROM flagged
)
-- Multi-level aggregation
SELECT
    page,
    CASE WHEN duration_sec >= 457 THEN 'LARGE' ELSE 'SMALL' END AS duration_sec_tier,
    AVG(duration_sec) AS avg_duration_sec,
    COUNT(*) FILTER (WHERE event_type = 'page_view') AS page_view_rows,
    COUNT(CASE WHEN event_type = 'click' THEN 1 END) AS click_count
FROM sessions_web_events
GROUP BY page, CASE WHEN duration_sec >= 457 THEN 'LARGE' ELSE 'SMALL' END
ORDER BY click_count DESC NULLS LAST;
