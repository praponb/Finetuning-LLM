SELECT * FROM (SELECT event_type, AVG(duration_sec) AS avg_duration_sec FROM web_events WHERE event_type <> 'page_view' GROUP BY event_type) AS x WHERE avg_duration_sec > 270;
