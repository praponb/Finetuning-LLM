WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        device,
        browser,
        country,
        TRIM(country) AS clean_country,
        SUBSTRING(browser, 1, 2) AS browser_prefix
    FROM sessions
    WHERE browser IN ('Chrome', 'Firefox', 'Safari')
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        SUBSTRING(device, 1, 3) AS device_prefix,
        COALESCE(browser, 'DIRECT') AS browser_filled
    FROM valid
    WHERE browser IN ('Edge', 'Firefox', 'Safari')
),
cleaned_sessions AS (
    -- Keep recent, valid rows
    SELECT
        id,
        device,
        browser,
        country,
        clean_country,
        browser_prefix,
        CONCAT_WS('-', browser, device) AS browser_device_key,
        LOWER(device) AS device_lower,
        COALESCE(browser, 'UNKNOWN') AS browser_clean
    FROM cleaned
    WHERE customer_id IS NOT NULL
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM cleaned_sessions s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events x
            WHERE x.session_id = s.id
              AND x.event_time >= '2023-10-01'
        )
),
valid_sessions AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CASE WHEN device = 'mobile' THEN 1 ELSE 0 END AS is_mobile,
        CONCAT_WS('-', country, device) AS country_device_key,
        COALESCE(browser, 'N/A') AS browser_clean_2
    FROM eligible
    WHERE country IS NOT NULL
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM valid_sessions s
    WHERE EXISTS (
            SELECT 1
            FROM web_events x
            WHERE x.session_id = s.id
              AND x.event_time >= '2025-10-01'
        )
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(3) OVER (PARTITION BY browser ORDER BY is_mobile, id) AS is_mobile_bucket,
        SUM(is_mobile) OVER (PARTITION BY browser ORDER BY is_mobile, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_is_mobile,
        AVG(is_mobile) OVER (PARTITION BY browser ORDER BY is_mobile, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS is_mobile_avg_3_prior
    FROM not_flagged
)
-- Aggregate with conditional metrics
SELECT
    country,
    browser,
    ROUND(SUM(running_is_mobile) / NULLIF(COUNT(CASE WHEN browser = 'Chrome' THEN 1 END), 0), 2) AS running_is_mobile_per_chrome,
    PERCENTILE_APPROX(running_is_mobile, ARRAY(0.25, 0.5, 0.75)) AS running_is_mobile_quartiles,
    SUM(is_mobile) AS total_is_mobile,
    SIZE(COLLECT_SET(country)) AS num_countries,
    GROUPING_ID() AS gid
FROM metrics
GROUP BY ROLLUP (country, browser)
ORDER BY total_is_mobile DESC
