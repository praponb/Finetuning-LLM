SELECT
    id,
    session_id,
    duration_sec,
    event_type
FROM (
    SELECT *, DENSE_RANK() OVER (PARTITION BY session_id ORDER BY duration_sec DESC) rn
    FROM web_events
) r
WHERE rn <= 5;
