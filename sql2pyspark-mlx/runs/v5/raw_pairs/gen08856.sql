SELECT session_id, duration_sec_range
FROM (
    SELECT session_id, MAX(duration_sec) - MIN(duration_sec) AS duration_sec_range
    FROM web_events
    WHERE duration_sec <> 425
    GROUP BY session_id
) AS s
WHERE duration_sec_range >= 57;
