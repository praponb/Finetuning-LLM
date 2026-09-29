SELECT * FROM web_events QUALIFY ROW_NUMBER() OVER (PARTITION BY page ORDER BY duration_sec DESC, id) = 1
