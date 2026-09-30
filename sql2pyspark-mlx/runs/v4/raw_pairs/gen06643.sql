WITH valid_web_events AS (
    SELECT *
    FROM web_events
    WHERE TO_DATE(event_time) < '2023-03-01'
),
web_events_stats AS (
    SELECT page, MIN(duration_sec) AS min_duration_sec, COUNT(CASE WHEN event_type = 'click' THEN 1 END) AS click_count
    FROM valid_web_events
    GROUP BY page
)
SELECT *
FROM web_events_stats
ORDER BY click_count DESC, page
LIMIT 10
