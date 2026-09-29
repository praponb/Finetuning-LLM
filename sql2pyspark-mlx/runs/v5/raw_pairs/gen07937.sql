WITH top_sessions AS (
    SELECT
        id,
        country,
        started_at,
        device,
        ROW_NUMBER() OVER (PARTITION BY country ORDER BY started_at DESC, id) AS rn
    FROM sessions
    WHERE country IS NULL
)
SELECT
    id,
    country,
    started_at,
    device
FROM top_sessions
WHERE rn = 1;
