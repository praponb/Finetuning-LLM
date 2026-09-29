WITH base AS (
    SELECT
        *,
        SUBSTRING(event_type, 1, 2) AS event_type_prefix,
        COALESCE(page, 'UNKNOWN') AS resolved_page
    FROM web_events
    WHERE event_time >= TO_DATE('2023-08-12')
      AND page IS NOT NULL
),
flagged AS (
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY session_id ORDER BY event_time, id) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY session_id ORDER BY event_time, id) + INTERVAL 60 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM base
),
islands AS (
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY session_id ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
filtered AS (
    SELECT
        id,
        event_type,
        duration_sec,
        event_type_prefix,
        is_new_burst,
        session_no,
        CONCAT_WS('-', page, event_type) AS page_event_type_key
    FROM islands
    WHERE event_time >= '2023-06-18'
),
base_web_events AS (
    SELECT
        id,
        duration_sec,
        event_type_prefix,
        is_new_burst,
        page_event_type_key,
        CASE WHEN session_no >= 7 THEN 'High' WHEN session_no >= 1 THEN 'Medium' ELSE 'Low' END AS session_no_band
    FROM filtered
    WHERE duration_sec >= 173
)
SELECT
    id,
    duration_sec,
    event_type_prefix,
    is_new_burst,
    page_event_type_key,
    session_no_band
FROM base_web_events;
