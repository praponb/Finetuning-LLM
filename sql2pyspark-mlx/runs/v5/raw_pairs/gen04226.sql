WITH base AS (
    SELECT *, COALESCE(duration_sec, 0) AS duration_sec_filled
    FROM web_events
    WHERE event_type = 'purchase'
)
SELECT id
FROM base
EXCEPT DISTINCT
SELECT id
FROM web_events
WHERE duration_sec >= 270;
