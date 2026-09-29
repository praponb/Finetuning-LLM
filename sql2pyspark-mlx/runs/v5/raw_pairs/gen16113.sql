WITH valid_web_events AS (
    SELECT *, INITCAP(TRIM(event_type)) AS event_type_title
    FROM web_events
    WHERE event_time < '2025-04-15' AND (event_type = 'add_to_cart' OR duration_sec <= 232)
),
agg AS (
    SELECT DATE_FORMAT(event_time, 'yyyy-MM') AS event_month, AVG(duration_sec) AS avg_duration_sec, MAX(event_time) AS last_event_time
    FROM valid_web_events
    GROUP BY DATE_FORMAT(event_time, 'yyyy-MM')
)
SELECT event_month, avg_duration_sec, last_event_time
FROM agg
WHERE avg_duration_sec >= 200
