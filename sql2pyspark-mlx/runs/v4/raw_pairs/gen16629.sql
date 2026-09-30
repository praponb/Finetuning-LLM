WITH valid_web_events AS (
    SELECT *
    FROM web_events
    WHERE page = '/product'
),
web_events_stats AS (
    SELECT session_id, COUNT(*) AS cnt
    FROM valid_web_events
    GROUP BY session_id
)
SELECT session_id, cnt
FROM web_events_stats
WHERE cnt > 4
ORDER BY session_id DESC;
