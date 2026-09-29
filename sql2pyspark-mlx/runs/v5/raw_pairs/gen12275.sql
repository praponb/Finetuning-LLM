WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        device,
        browser,
        started_at,
        CONCAT(device, ' - ', country) AS device_country,
        DATE_FORMAT(started_at, 'MMM yyyy') AS started_at_label
    FROM sessions
    WHERE country IS NOT NULL
),
kept_keys AS (
    -- Set difference on keys
    SELECT browser
    FROM cleaned
    EXCEPT
    SELECT browser
    FROM sessions
    WHERE started_at < '2023-08-01'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM cleaned s
    INNER JOIN kept_keys k ON s.browser = k.browser
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        LOWER(device) AS device_lower,
        COALESCE(browser, 'NONE') AS browser_filled
    FROM verified
    WHERE started_at >= DATE '2023-07-21'
      AND browser IS NOT NULL
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM filtered s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events ch
            WHERE ch.session_id = s.id
              AND ch.event_time < s.started_at
        )
),
filtered_sessions AS (
    -- Keep recent, valid rows
    SELECT *, DATE_TRUNC('WEEK', started_at) AS started_at_period
    FROM not_flagged
    WHERE browser IS NOT DISTINCT FROM 'Safari'
      AND device = 'desktop'
),
remaining_keys AS (
    -- Set difference on keys
    SELECT id, browser
    FROM filtered_sessions
    EXCEPT
    SELECT id, browser
    FROM sessions
    WHERE device = 'desktop'
),
verified_sessions AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM filtered_sessions s
    INNER JOIN remaining_keys k ON s.id = k.id AND s.browser = k.browser
)
SELECT
    browser,
    device_country,
    device_lower,
    browser_filled,
    started_at_period,
    CASE WHEN browser RLIKE '^(Edge|Chrome)$' THEN 'EXTENDED' WHEN browser RLIKE '^(Safari|Firefox)$' THEN 'GROUP_A' ELSE 'OTHER_ZONE' END AS browser_zone
FROM verified_sessions
ORDER BY browser DESC;
