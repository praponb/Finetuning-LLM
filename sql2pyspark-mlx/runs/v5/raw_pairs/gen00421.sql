SELECT
    id,
    device,
    started_at,
    LEAD(started_at, 1) OVER (PARTITION BY device ORDER BY started_at, id) next_started_at
FROM sessions
ORDER BY device ASC, next_started_at DESC
