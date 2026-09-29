SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY session_id, event_type ORDER BY event_time ASC, id ASC) AS row_num
    FROM web_events
) x
WHERE row_num = 1;
