WITH web_events_by_session AS (
    SELECT session_id, MAX(duration_sec) - MIN(duration_sec) AS duration_sec_range, MIN(duration_sec) AS min_duration_sec
    FROM web_events
    GROUP BY session_id
)
SELECT
    s.id,
    s.started_at,
    webs.duration_sec_range,
    webs.min_duration_sec
FROM sessions s
LEFT JOIN web_events_by_session webs ON s.id = webs.session_id;
