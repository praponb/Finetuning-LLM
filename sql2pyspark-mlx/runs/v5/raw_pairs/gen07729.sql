WITH SessionTotals AS (
    SELECT session_id, ROUND(STDDEV(duration_sec), 2) AS stddev_duration_sec, AVG(duration_sec) AS avg_duration_sec
    FROM web_events
    GROUP BY session_id
)
SELECT
    s.id,
    s.started_at,
    S.stddev_duration_sec,
    S.avg_duration_sec
FROM sessions s
LEFT OUTER JOIN SessionTotals S ON s.id = S.session_id
WHERE S.stddev_duration_sec > 68
ORDER BY s.started_at;
