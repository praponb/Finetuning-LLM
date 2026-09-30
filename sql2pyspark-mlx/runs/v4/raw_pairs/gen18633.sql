WITH Numbered AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY browser ORDER BY started_at DESC, id) AS pos
    FROM sessions
)
SELECT
    id,
    browser,
    started_at,
    customer_id
FROM Numbered
WHERE pos = 1
