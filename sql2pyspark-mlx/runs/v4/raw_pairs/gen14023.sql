WITH ranked_sessions AS (
    SELECT
        id,
        country,
        started_at,
        browser,
        DENSE_RANK() OVER (PARTITION BY country ORDER BY started_at) row_num
    FROM sessions
)
SELECT
    id,
    country,
    started_at,
    browser
FROM ranked_sessions
WHERE row_num <= 2
ORDER BY id DESC
