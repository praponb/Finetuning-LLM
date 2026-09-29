SELECT QUARTER(event_time) AS event_quarter, COUNT(duration_sec) AS duration_sec_count FROM web_events GROUP BY 1 HAVING COUNT(duration_sec) < 5 ORDER BY duration_sec_count;
