WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        device,
        browser,
        started_at,
        country,
        CONCAT(device, ' - ', browser) AS device_browser
    FROM sessions
    WHERE country IS DISTINCT FROM 'TH'
)
-- Deduplicate with ROW_NUMBER
SELECT *
FROM base
QUALIFY ROW_NUMBER() OVER (PARTITION BY device ORDER BY started_at DESC NULLS FIRST, id ASC) = 1
