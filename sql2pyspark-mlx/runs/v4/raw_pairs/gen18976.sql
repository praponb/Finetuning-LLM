WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        session_id,
        event_type,
        page,
        event_time,
        duration_sec,
        LOWER(event_type) AS event_type_lower
    FROM web_events
    WHERE event_time > TIMESTAMP '2024-05-14 10:00:00'
      AND page IS NOT NULL
      AND TO_DATE(event_time) >= '2024-03-06'
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY session_id ORDER BY event_time, id) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY session_id ORDER BY event_time, id) + INTERVAL '14' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM valid
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY session_id ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM with_gaps
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        session_id,
        event_type,
        page,
        duration_sec,
        event_type_lower,
        is_new_burst,
        session_no,
        session_no * duration_sec AS session_no_times_duration_sec,
        CASE WHEN duration_sec >= 419 THEN 'High' WHEN duration_sec >= 206 THEN 'Medium' ELSE 'Low' END AS duration_sec_level,
        COALESCE(page, 'NONE') AS page_clean
    FROM islands
    WHERE event_time >= TO_DATE('2024-02-25')
)
SELECT
    s.id,
    s.session_id,
    s.event_type,
    s.page,
    s.duration_sec,
    s.event_type_lower,
    s.session_no,
    s.duration_sec_level,
    (SELECT AVG(t2.duration_sec) FROM web_events t2 WHERE t2.session_id = s.session_id) AS avg_web_event_duration_sec
FROM filtered s
