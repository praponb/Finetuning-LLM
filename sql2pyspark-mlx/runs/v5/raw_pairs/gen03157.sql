WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        browser,
        started_at,
        country,
        INITCAP(TRIM(device)) device_title,
        COALESCE(country, 'N/A') resolved_country
    FROM sessions
    WHERE browser IS NOT NULL
),
filtered_sessions AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        browser,
        started_at,
        country,
        device_title,
        CONCAT_WS(',', browser, country) browser_country_key
    FROM filtered
    WHERE started_at >= TIMESTAMP '2023-09-02 05:00:00'
      AND country IS NOT NULL
),
remaining_keys AS (
    -- Set difference on keys
    SELECT browser, id
    FROM filtered_sessions
    EXCEPT
    SELECT browser, id
    FROM sessions
    WHERE country = 'TH'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM filtered_sessions s
    INNER JOIN remaining_keys k ON s.browser = k.browser AND s.id = k.id
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CONCAT_WS('-', browser, country) browser_country_key_2,
        DATE_FORMAT(started_at, 'yyyy-MM-dd') started_at_label,
        COALESCE(country, 'UNKNOWN') country_filled
    FROM kept_rows
    WHERE started_at >= TO_TIMESTAMP('2023-08-22 08:00:00')
      AND country IS NOT NULL
),
cleaned_sessions AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CONCAT(browser, ' - ', country) browser_country,
        CASE WHEN browser IN ('Firefox', 'Chrome') THEN 'Core' ELSE 'Other' END browser_group
    FROM cleaned
    WHERE started_at > TO_DATE('2023-03-31')
),
filtered_sessions_2 AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        browser,
        started_at,
        country,
        device_title,
        browser_country_key,
        browser_country_key_2,
        started_at_label,
        country_filled,
        browser_country,
        browser_group,
        DATE_TRUNC('week', started_at) started_at_period,
        SUBSTRING(country, 1, 3) country_prefix
    FROM cleaned_sessions
    WHERE browser = 'Edge'
      AND browser IS NOT DISTINCT FROM 'Chrome'
),
cleaned_sessions_2 AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        DATE_FORMAT(started_at, 'yyyy-MM-dd') started_at_label_2,
        UPPER(country) country_upper
    FROM filtered_sessions_2
    WHERE started_at >= TIMESTAMP '2023-03-28 20:00:00'
      AND browser != 'Safari'
),
report AS (
    SELECT
        browser,
        started_at,
        country,
        device_title,
        browser_country_key,
        browser_country_key_2,
        started_at_label,
        browser_country,
        started_at_label_2,
        CASE WHEN country RLIKE '^(UK|DE)$' THEN 'GROUP_A' WHEN country RLIKE '^(TH|US)$' THEN 'CORE' ELSE 'GROUP_B' END AS country_zone
    FROM cleaned_sessions_2
)
SELECT
    browser,
    started_at,
    device_title,
    browser_country_key,
    browser_country_key_2,
    started_at_label,
    started_at_label_2,
    country_zone
FROM report
ORDER BY started_at DESC NULLS FIRST, country_zone DESC;
