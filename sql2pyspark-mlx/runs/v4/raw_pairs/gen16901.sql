WITH web_events_by_session AS (
    SELECT session_id, SUM(duration_sec) AS total_duration_sec, COUNT(duration_sec) AS duration_sec_count
    FROM web_events
    GROUP BY session_id
)
SELECT
    s.country,
    s.device,
    webs.total_duration_sec,
    webs.duration_sec_count
FROM sessions s
INNER JOIN web_events_by_session webs ON s.id = webs.session_id
WHERE webs.duration_sec_count >= 4;
