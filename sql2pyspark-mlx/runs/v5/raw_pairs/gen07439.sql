SELECT
    id,
    duration_sec,
    event_time,
    CASE WHEN duration_sec >= 425 THEN 'High' WHEN duration_sec >= 236 THEN 'Medium' ELSE 'Low' END AS duration_sec_level
FROM web_events
WHERE HOUR(event_time) BETWEEN 6 AND 13 OR duration_sec IS NOT NULL
ORDER BY duration_sec, id
LIMIT 1
