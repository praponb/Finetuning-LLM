SELECT
    id,
    started_at,
    device,
    country
FROM sessions
WHERE TO_DATE(started_at) >= '2024-05-01';
