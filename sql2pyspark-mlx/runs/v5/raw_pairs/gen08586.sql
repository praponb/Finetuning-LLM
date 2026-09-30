SELECT page, QUARTER(event_time) AS event_quarter, MAX(duration_sec) AS max_duration_sec
FROM web_events
WHERE session_id IN (4, 8, 12) AND event_type = 'page_view' AND TO_DATE(event_time) < '2024-09-16'
GROUP BY page, QUARTER(event_time)
HAVING max_duration_sec < 390
ORDER BY event_quarter;
