SELECT id, page, duration_sec, RANK() OVER (PARTITION BY page ORDER BY duration_sec) AS rnk FROM web_events ORDER BY duration_sec DESC;
