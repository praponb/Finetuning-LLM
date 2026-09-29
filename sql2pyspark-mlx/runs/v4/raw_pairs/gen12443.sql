WITH recent_web_events AS (
    SELECT *, ROUND(duration_sec, 2) AS duration_sec_rounded
    FROM web_events
    WHERE event_time < '2025-03-30'
),
web_events_stats AS (
    SELECT event_type, page, SUM(CASE WHEN page = '/product' THEN duration_sec_rounded ELSE 0 END) AS product_duration_sec_rounded
    FROM recent_web_events
    GROUP BY event_type, page
)
SELECT *
FROM web_events_stats
ORDER BY product_duration_sec_rounded DESC
