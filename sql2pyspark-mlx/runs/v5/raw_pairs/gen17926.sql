WITH numbered AS (
    SELECT
        id,
        country,
        started_at,
        device,
        DENSE_RANK() OVER (PARTITION BY country ORDER BY started_at) AS pos
    FROM sessions
    WHERE country = 'JP'
)
SELECT
    id,
    country,
    started_at,
    device
FROM numbered
WHERE pos <= 3
ORDER BY 1 DESC, started_at DESC;
