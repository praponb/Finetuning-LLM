SELECT
    id,
    country,
    started_at,
    device
FROM (
    SELECT
        id,
        country,
        started_at,
        device,
        ROW_NUMBER() OVER (PARTITION BY country ORDER BY started_at, id) AS row_num
    FROM sessions
    WHERE device = 'mobile'
) AS r
WHERE row_num <= 3;
