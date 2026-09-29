WITH with_dims AS (
    -- Attach sessions attributes
    SELECT
        e.id,
        e.session_id,
        e.event_time,
        d.browser,
        d.device
    FROM web_events e
    LEFT JOIN sessions d
        ON e.session_id = d.id
),
kept_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM with_dims
    INTERSECT
    SELECT id
    FROM web_events
    WHERE TO_DATE(event_time) >= '2023-05-01'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_dims s
    INNER JOIN kept_keys k ON s.id = k.id
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        session_id,
        event_time,
        browser,
        device,
        COALESCE(browser, 'Unknown') AS browser_filled,
        COALESCE(device, 'N/A') AS device_clean
    FROM verified
    WHERE event_time >= DATE '2023-06-10'
      AND device IS NOT NULL
),
enriched AS (
    -- Attach sessions attributes
    SELECT
        e.*,
        b.browser AS session_browser,
        b.device AS session_device,
        b.country
    FROM base e
    LEFT JOIN sessions b
        ON e.session_id = b.id
    WHERE b.customer_id = 28
)
SELECT
    session_id,
    event_time,
    browser,
    device,
    browser_filled,
    device_clean,
    session_device,
    country,
    CASE WHEN device RLIKE '^(desktop|mobile)$' THEN 'EXTENDED' ELSE 'GROUP_B' END AS device_zone
FROM enriched;
