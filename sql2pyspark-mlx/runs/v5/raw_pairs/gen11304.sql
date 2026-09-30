WITH cleaned AS (
    SELECT *, CAST(event_time AS DATE) AS event_time_day
    FROM web_events
    WHERE event_time > TO_DATE('2023-08-13')
),
enriched AS (
    SELECT
        e.*,
        b.country,
        b.browser,
        b.device
    FROM cleaned e
    LEFT JOIN sessions b
        ON e.session_id = b.id
),
gaps AS (
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY event_type ORDER BY event_time, id) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY event_type ORDER BY event_time, id) + INTERVAL 90 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM enriched
),
sessions_web_events AS (
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY event_type ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM gaps
),
with_prev AS (
    SELECT *, LAG(event_time) OVER (PARTITION BY device ORDER BY event_time, id) AS prev_event_time
    FROM sessions_web_events
),
flagged AS (
    SELECT *, CASE WHEN prev_event_time IS NULL OR event_time > prev_event_time + INTERVAL 60 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM with_prev
),
sessions_web_events_2 AS (
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY device ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged
)
SELECT
    id,
    session_id,
    event_type,
    page,
    duration_sec,
    event_time_day,
    browser,
    is_new_burst,
    burst_id
FROM sessions_web_events_2
ORDER BY event_type DESC;
