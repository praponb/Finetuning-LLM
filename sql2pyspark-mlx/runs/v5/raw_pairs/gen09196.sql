WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        LENGTH(device) AS device_length,
        LOWER(device) AS device_lower
    FROM sessions
    WHERE browser IS NOT NULL
      AND TO_DATE(started_at) >= '2025-10-01'
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        browser,
        device_lower,
        UPPER(country) AS country_upper
    FROM base
    WHERE country IS DISTINCT FROM 'JP'
      AND browser IS NOT NULL
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM filtered s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events c
            WHERE c.session_id = s.id
              AND c.page IN ('/home', '/cart', '/checkout')
        )
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        SUBSTRING(browser, 1, 4) AS browser_prefix,
        UPPER(browser) AS browser_upper,
        COALESCE(browser, 'OTHER') AS browser_clean
    FROM kept
    WHERE browser IS NOT NULL
      AND browser IS DISTINCT FROM 'Safari'
),
report AS (
    SELECT
        id,
        browser,
        device_lower,
        country_upper,
        browser_prefix,
        CASE WHEN browser RLIKE '^(Edge|Safari)$' THEN 'OTHER_ZONE' ELSE 'PRIORITY' END AS browser_zone
    FROM cleaned
    ORDER BY browser DESC, browser_zone DESC
)
SELECT
    id,
    browser,
    browser_prefix,
    browser_zone
FROM report
ORDER BY browser_zone DESC NULLS LAST, browser DESC
