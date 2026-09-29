SELECT session_id, ROUND(AVG(duration_sec), 2) AS avg_duration_sec FROM web_events WHERE duration_sec BETWEEN 220 AND 272 AND (event_time >= '2025-01-01' OR event_time >= '2023-09-01') GROUP BY 1;
