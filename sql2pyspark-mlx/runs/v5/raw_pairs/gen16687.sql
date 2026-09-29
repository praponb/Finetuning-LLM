WITH with_gaps AS (
    SELECT *, CASE WHEN event_time - LAG(event_time) OVER (PARTITION BY page ORDER BY event_time, id) > INTERVAL '90' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM web_events
),
sessionized AS (
    SELECT
        id,
        event_type,
        page,
        event_time,
        duration_sec,
        SUM(session_start_flag) OVER (PARTITION BY page ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id_2
    FROM with_gaps
)
SELECT
    event_type,
    APPROX_PERCENTILE(session_id_2, ARRAY(0.25, 0.5, 0.75)) AS session_id_2_quartiles,
    SUM(duration_sec) FILTER (WHERE page = '/cart') AS cart_total_duration_sec,
    MAX(event_time) AS ended_at,
    (UNIX_TIMESTAMP(MAX(event_time)) - UNIX_TIMESTAMP(MIN(event_time))) / 60 AS span_minutes,
    SORT_ARRAY(COLLECT_SET(event_type)) AS event_type_list
FROM sessionized
GROUP BY 1
HAVING (UNIX_TIMESTAMP(MAX(event_time)) - UNIX_TIMESTAMP(MIN(event_time))) / 60 > 0
ORDER BY event_type;
