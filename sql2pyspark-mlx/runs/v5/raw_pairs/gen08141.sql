SELECT
    id,
    device,
    browser,
    country
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY device ORDER BY started_at DESC, id ASC) AS rn
    FROM sessions
) t
WHERE rn = 1
