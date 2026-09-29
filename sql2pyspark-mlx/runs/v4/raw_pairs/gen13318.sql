SELECT id, event_type, duration_sec
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY duration_sec DESC, id) AS rnk
    FROM web_events
) AS t
WHERE rnk <= 2;
