SELECT * FROM (SELECT *, DENSE_RANK() OVER (PARTITION BY page ORDER BY event_time DESC) AS rn FROM web_events) AS ranked WHERE rn <= 3 ORDER BY duration_sec DESC, session_id DESC;
