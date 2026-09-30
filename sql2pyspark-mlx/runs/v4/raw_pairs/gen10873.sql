WITH numbered AS (
    SELECT
        id,
        browser,
        started_at,
        ROW_NUMBER() OVER (PARTITION BY browser ORDER BY started_at DESC, id) AS rnk
    FROM sessions
)
SELECT *
FROM numbered
WHERE rnk <= 1
ORDER BY started_at, browser
