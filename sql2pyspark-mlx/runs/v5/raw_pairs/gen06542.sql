WITH with_gaps AS (
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY session_id ORDER BY event_time, id) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY session_id ORDER BY event_time, id) + INTERVAL 14 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM web_events
),
islands AS (
    SELECT
        id,
        session_id,
        event_time,
        duration_sec,
        SUM(is_new_burst) OVER (PARTITION BY session_id ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM with_gaps
),
joined AS (
    SELECT
        e.id,
        e.session_id,
        e.event_time,
        e.session_no,
        s.device,
        s.country
    FROM islands e
    LEFT JOIN sessions s
        ON e.session_id = s.id
    WHERE s.device IN ('tablet', 'desktop')
),
gaps AS (
    SELECT *, CASE WHEN event_time - LAG(event_time) OVER (PARTITION BY device ORDER BY event_time, id) > INTERVAL '14' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM joined
),
islands_web_events AS (
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY device ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id_2
    FROM gaps
),
with_gaps_2 AS (
    SELECT *, CASE WHEN event_time - LAG(event_time) OVER (PARTITION BY country ORDER BY event_time, id) > INTERVAL 90 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM islands_web_events
),
sessionized AS (
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY country ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no_2
    FROM with_gaps_2
)
SELECT
    dim.device AS session_device,
    dim.country AS session_country,
    src.device AS web_event_device,
    COUNT(*) AS num_rows,
    ROUND(SUM(src.session_no), 2) AS total_session_no,
    SUM(src.session_no_2) AS total_session_no_2
FROM sessionized src
JOIN sessions dim ON src.session_id = dim.id
GROUP BY dim.device, dim.country, src.device
