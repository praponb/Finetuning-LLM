WITH latest_match AS (
    SELECT s.*, x.web_event_event_type
    FROM sessions s
    LEFT JOIN LATERAL (
            SELECT c.event_type AS web_event_event_type
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.duration_sec DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
SELECT
    DATE_TRUNC('QUARTER', started_at) AS quarter_start,
    web_event_event_type,
    MAX(started_at) AS ended_at,
    MAX(started_at) AS ended_at_2,
    MAX(started_at) AS ended_at_3,
    MIN(started_at) AS started_at,
    SORT_ARRAY(COLLECT_SET(browser)) AS browser_list,
    COUNT(*) AS row_count
FROM latest_match
GROUP BY GROUPING SETS ((DATE_TRUNC('QUARTER', started_at), web_event_event_type), (DATE_TRUNC('QUARTER', started_at)))
HAVING COUNT(*) >= 1
ORDER BY row_count ASC;
