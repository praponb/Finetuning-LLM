WITH filtered AS (
    SELECT
        *,
        CONCAT(browser, ' - ', device) AS browser_device,
        SUBSTRING(country, 1, 2) AS country_prefix,
        COALESCE(country, 'N/A') AS country_filled
    FROM sessions
    WHERE country IS NOT NULL
      AND device <> 'tablet'
)
SELECT
    id,
    customer_id,
    device,
    browser,
    started_at,
    browser_device,
    country_prefix,
    country_filled
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY browser ORDER BY started_at DESC NULLS FIRST, id ASC) AS row_num
    FROM filtered
) x
WHERE row_num = 1
