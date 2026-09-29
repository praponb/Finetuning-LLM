WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        TRIM(country) AS clean_country,
        COALESCE(browser, 'OTHER') AS browser_filled
    FROM sessions
    WHERE started_at >= TIMESTAMP '2023-10-15 21:00:00'
      AND country IS NOT NULL
)
SELECT device, country
FROM valid
EXCEPT
SELECT device, country
FROM sessions
WHERE country IN ('US', 'DE', 'TH');
