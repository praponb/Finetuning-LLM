WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        HOUR(event_time) AS event_time_hour,
        CASE WHEN event_type IN ('click', 'search') THEN 'Core' ELSE 'Other' END AS event_type_group
    FROM web_events
    WHERE event_time > DATE '2023-08-13'
      AND page IS NOT DISTINCT FROM '/search'
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CAST(event_time_hour AS DOUBLE) AS event_time_hour_str,
        CAST(duration_sec AS STRING) AS duration_sec_num,
        COALESCE(page, 'N/A') AS page_filled
    FROM cleaned
    WHERE event_time_hour <= 33
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN event_time - LAG(event_time) OVER (PARTITION BY page ORDER BY event_time, id) > INTERVAL 60 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM valid
),
sessions_web_events AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        page,
        event_time,
        duration_sec,
        event_time_hour,
        event_time_hour_str,
        duration_sec_num,
        SUM(is_new_session) OVER (PARTITION BY page ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM gaps
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        page,
        event_time,
        duration_sec,
        event_time_hour,
        event_time_hour_str,
        duration_sec_num,
        session_no,
        DATE_FORMAT(event_time, 'yyyy-MM') AS event_time_label
    FROM sessions_web_events
    WHERE event_time > TIMESTAMP '2023-05-23 05:00:00'
)
SELECT
    id,
    page,
    event_time,
    event_time_hour_str,
    duration_sec_num,
    session_no,
    event_time_label
FROM base
