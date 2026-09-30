WITH with_dims AS (
    SELECT
        src.*,
        d.browser,
        d.country,
        d.device
    FROM web_events src
    JOIN sessions d
        ON src.session_id = d.id
)
SELECT
    id,
    page,
    event_time,
    duration_sec,
    country
FROM with_dims
ORDER BY event_time DESC;
