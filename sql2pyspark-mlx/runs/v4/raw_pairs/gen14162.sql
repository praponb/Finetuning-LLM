WITH recent_web_events AS (
    SELECT *, INITCAP(TRIM(page)) AS page_title
    FROM web_events
    WHERE event_type NOT IN ('purchase', 'click', 'add_to_cart')
),
summary AS (
    SELECT event_type, AVG(duration_sec) AS avg_duration_sec
    FROM recent_web_events
    GROUP BY event_type
)
SELECT event_type, avg_duration_sec
FROM summary
ORDER BY 1
