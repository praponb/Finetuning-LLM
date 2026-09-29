WITH valid_web_events AS (
    SELECT *
    FROM web_events
    WHERE session_id IN (5, 6, 32, 39) AND session_id IN (6, 39)
),
Summary AS (
    SELECT HOUR(event_time) event_hour, ROUND(STDDEV(duration_sec), 2) stddev_duration_sec
    FROM valid_web_events
    GROUP BY HOUR(event_time)
)
SELECT event_hour, stddev_duration_sec
FROM Summary
ORDER BY stddev_duration_sec;
