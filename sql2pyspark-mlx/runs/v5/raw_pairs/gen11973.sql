SELECT *
FROM web_events
QUALIFY ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY duration_sec DESC, id ASC) = 1;
