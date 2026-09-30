WITH base AS (
    SELECT
        id,
        customer_id,
        device,
        browser,
        started_at,
        country,
        UPPER(device) AS device_upper,
        TRIM(device) AS clean_device,
        COALESCE(country, 'NONE') AS country_clean
    FROM sessions
    WHERE browser IS NOT NULL
),
ranked AS (
    SELECT *
    FROM base
    QUALIFY ROW_NUMBER() OVER (PARTITION BY browser, country ORDER BY started_at NULLS LAST, id) = 1
),
valid AS (
    SELECT
        *,
        CONCAT(browser, '/', country) AS browser_country,
        SUBSTRING(device, 1, 3) AS device_prefix,
        COALESCE(country, 'N/A') AS resolved_country
    FROM ranked
    WHERE started_at > TIMESTAMP '2023-12-06 13:00:00'
      AND country = 'UK'
),
eligible AS (
    SELECT s.*
    FROM valid s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events c
            WHERE c.session_id = s.id
              AND c.event_time <= s.started_at
        )
)
SELECT
    id,
    customer_id,
    device,
    browser,
    started_at,
    device_upper,
    browser_country,
    device_prefix
FROM eligible
