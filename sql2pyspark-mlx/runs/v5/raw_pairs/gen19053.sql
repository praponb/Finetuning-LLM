SELECT session_id, ROUND(AVG(duration_sec), 2) AS avg_duration_sec
FROM web_events
WHERE duration_sec BETWEEN 152 AND 210
GROUP BY session_id
ORDER BY avg_duration_sec
