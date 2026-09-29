WITH ranked_sessions AS (
    SELECT
        id,
        browser,
        started_at,
        ROW_NUMBER() OVER (PARTITION BY browser ORDER BY started_at DESC, id) AS rnk
    FROM sessions
)
SELECT id, browser, started_at
FROM ranked_sessions
WHERE rnk <= 3;
