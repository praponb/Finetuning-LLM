WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        device,
        started_at,
        UPPER(browser) AS browser_upper
    FROM sessions
    WHERE country = 'SG'
)
SELECT id, device
FROM cleaned
INTERSECT
SELECT id, device
FROM sessions
WHERE country IS NOT NULL;
