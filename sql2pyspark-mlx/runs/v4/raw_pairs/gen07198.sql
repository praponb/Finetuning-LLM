WITH with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        MIN(duration_sec) OVER (PARTITION BY event_type) AS event_type_min_duration_sec,
        LAST_VALUE(duration_sec, TRUE) OVER (PARTITION BY session_id ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_duration_sec
    FROM web_events
)
SELECT
    id,
    event_type,
    page,
    event_time,
    event_type_min_duration_sec,
    last_known_duration_sec
FROM with_windows
ORDER BY id DESC NULLS LAST
LIMIT 10;
