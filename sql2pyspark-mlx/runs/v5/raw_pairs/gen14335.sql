WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        HOUR(event_time) AS event_time_hour,
        COALESCE(page, 'DIRECT') AS page_clean
    FROM web_events
    WHERE page IS NOT NULL
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        session_id,
        event_type,
        page,
        event_time,
        event_time_hour,
        page_clean,
        CAST(event_time_hour AS DOUBLE) AS event_time_hour_num,
        COALESCE(page, 'OTHER') AS page_clean_2
    FROM valid
    WHERE event_time > TO_DATE('2023-10-11')
      AND page IS NOT NULL
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY session_id ORDER BY event_time, id) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY session_id ORDER BY event_time, id) + INTERVAL '60' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM filtered
),
sessions_web_events AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        session_id,
        event_time,
        event_time_hour,
        page_clean,
        event_time_hour_num,
        page_clean_2,
        SUM(session_start_flag) OVER (PARTITION BY session_id ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
flagged_2 AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN event_time - LAG(event_time) OVER (PARTITION BY session_id ORDER BY event_time, id) > INTERVAL 30 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM sessions_web_events
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY session_id ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id_2
    FROM flagged_2
),
grouped AS (
    -- Grouped metrics
    SELECT
        CASE WHEN is_new_burst >= 1 THEN 'HIGH' ELSE 'LOW' END AS is_new_burst_tier,
        session_no,
        APPROX_PERCENTILE(event_time_hour_num, ARRAY(0.25, 0.5, 0.75)) AS event_time_hour_num_quartiles,
        MAX(event_time) AS last_event_time,
        MAX(event_time) AS last_event_time_2,
        MAX(event_time) AS last_seen,
        (UNIX_TIMESTAMP(MAX(event_time)) - UNIX_TIMESTAMP(MIN(event_time))) / 60 AS duration_min
    FROM islands
    GROUP BY is_new_burst_tier, session_no
)
SELECT
    is_new_burst_tier,
    session_no,
    event_time_hour_num_quartiles,
    last_event_time,
    last_seen
FROM grouped
