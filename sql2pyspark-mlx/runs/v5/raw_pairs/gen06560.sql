WITH valid_web_events AS (
    SELECT *
    FROM web_events
    WHERE page = '/search' AND TO_DATE(event_time) < '2023-12-29'
),
summary AS (
    SELECT QUARTER(event_time) event_quarter, SUM(CASE WHEN page = '/cart' THEN 1 ELSE 0 END) cart_count, MAX(duration_sec) max_duration_sec
    FROM valid_web_events
    GROUP BY QUARTER(event_time)
)
SELECT event_quarter, cart_count, max_duration_sec
FROM summary
