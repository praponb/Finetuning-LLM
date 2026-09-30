SELECT
    id,
    browser,
    country,
    started_at
FROM sessions
WHERE TO_DATE(started_at) < '2023-08-01'
