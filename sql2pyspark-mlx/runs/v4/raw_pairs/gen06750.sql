WITH flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY session_id ORDER BY event_time, id) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY session_id ORDER BY event_time, id) + INTERVAL 14 DAYS THEN 1 ELSE 0 END AS new_group_flag
    FROM web_events
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY session_id ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id_2
    FROM flagged
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        session_id,
        event_type,
        page,
        event_time,
        duration_sec,
        new_group_flag,
        session_id_2,
        AVG(duration_sec) OVER (PARTITION BY session_id ORDER BY new_group_flag, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS duration_sec_avg_3,
        SUM(new_group_flag) OVER (PARTITION BY page ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_new_group_flag
    FROM sessionized
),
flagged_2 AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY page ORDER BY event_time, id) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY page ORDER BY event_time, id) + INTERVAL '60' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM scored
),
sessions_web_events AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY page ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id_3
    FROM flagged_2
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        session_id,
        page,
        duration_sec
    FROM sessions_web_events
) src
PIVOT (
    MAX(duration_sec)
    FOR page IN ('/search' AS search, '/cart' AS cart)
)
