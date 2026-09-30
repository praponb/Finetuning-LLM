WITH top_sessions AS (
    SELECT
        id,
        browser,
        started_at,
        ROW_NUMBER() OVER (PARTITION BY browser ORDER BY started_at DESC, id) AS pos
    FROM sessions
)
SELECT id, browser, started_at
FROM top_sessions
WHERE pos <= 3
ORDER BY id DESC;
