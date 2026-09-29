WITH remaining_keys AS (
    -- Set difference on keys
    SELECT page, session_id
    FROM web_events
    EXCEPT
    SELECT page, session_id
    FROM web_events
    WHERE duration_sec IS NOT NULL
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM web_events s
    INNER JOIN remaining_keys k ON s.page = k.page AND s.session_id = k.session_id
)
SELECT
    id,
    session_id,
    event_type,
    page,
    event_time,
    duration_sec
FROM verified
