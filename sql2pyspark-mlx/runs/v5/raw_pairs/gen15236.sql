SELECT id, session_id, duration_sec
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY duration_sec DESC, id) AS rn
    FROM web_events
) AS r
WHERE rn = 1
ORDER BY id DESC
