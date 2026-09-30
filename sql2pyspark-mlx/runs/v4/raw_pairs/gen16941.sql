SELECT id, session_id, event_time
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY event_time DESC, id) AS rnk
    FROM web_events
    WHERE event_time < '2024-09-01'
) AS r
WHERE rnk = 1;
