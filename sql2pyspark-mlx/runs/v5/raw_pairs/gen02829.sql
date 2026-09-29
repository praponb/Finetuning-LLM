SELECT id, page, event_time FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY page ORDER BY event_time DESC, id) AS pos FROM web_events) AS t WHERE pos <= 3;
