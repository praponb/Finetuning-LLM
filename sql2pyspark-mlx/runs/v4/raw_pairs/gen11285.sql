WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        browser,
        country,
        CONCAT_WS('-', device, country) AS device_country_key
    FROM sessions
    WHERE browser IS NOT NULL
)
-- Aggregate with conditional metrics
SELECT
    customer_id,
    country,
    COUNT(*) FILTER (WHERE country = 'SG') AS sg_rows,
    COUNT(*) AS row_count,
    SIZE(COLLECT_SET(country)) AS num_countries,
    SIZE(COLLECT_SET(browser)) AS num_browsers
FROM cleaned
GROUP BY customer_id, country
HAVING COUNT(*) >= 1
ORDER BY country
