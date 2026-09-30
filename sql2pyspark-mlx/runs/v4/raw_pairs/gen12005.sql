SELECT
    id,
    country,
    started_at,
    ROW_NUMBER() OVER (PARTITION BY country ORDER BY started_at DESC, id) AS rn
FROM sessions
WHERE TO_DATE(started_at) >= '2023-03-01'
