SELECT
    id,
    browser,
    started_at,
    device
FROM (
    SELECT
        id,
        browser,
        started_at,
        device,
        ROW_NUMBER() OVER (PARTITION BY browser ORDER BY started_at DESC, id) AS rnk
    FROM sessions
    WHERE device IN ('tablet', 'desktop')
) AS r
WHERE rnk <= 5
ORDER BY id DESC, 4 DESC;
