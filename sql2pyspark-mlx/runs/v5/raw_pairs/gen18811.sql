SELECT s.country, MIN(we.event_time) AS min_event_time FROM web_events we LEFT JOIN sessions s ON s.id = we.session_id WHERE we.session_id = 16 GROUP BY s.country ORDER BY 2 DESC
