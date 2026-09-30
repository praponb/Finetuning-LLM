WITH numbered AS (
    SELECT
        id,
        device,
        started_at,
        browser,
        ROW_NUMBER() OVER (PARTITION BY device ORDER BY started_at DESC, id) rnk
    FROM sessions
)
SELECT
    id,
    device,
    started_at,
    browser
FROM numbered
WHERE rnk <= 5;
