WITH ranked_sessions AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY browser ORDER BY started_at DESC, id) AS pos
    FROM sessions
    WHERE started_at < '2024-03-01'
)
SELECT id, browser, started_at
FROM ranked_sessions
WHERE pos <= 1;
