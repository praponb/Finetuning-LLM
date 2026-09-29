WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CASE WHEN page IN ('/cart', '/checkout') THEN 'Core' ELSE 'Other' END AS page_group,
        COALESCE(page, 'NONE') AS page_filled
    FROM web_events
    WHERE event_time >= DATE '2023-10-27'
),
with_dims AS (
    -- Attach sessions attributes
    SELECT
        src.*,
        p.device,
        p.country,
        p.browser
    FROM filtered src
    LEFT JOIN sessions p
        ON src.session_id = p.id
),
enriched AS (
    -- Attach sessions attributes
    SELECT
        src.*,
        b.browser AS session_browser,
        b.country AS session_country
    FROM with_dims src
    LEFT JOIN sessions b
        ON src.session_id = b.id
),
matched_keys AS (
    -- Set intersection on keys
    SELECT page, event_type
    FROM enriched
    INTERSECT ALL
    SELECT page, event_type
    FROM web_events
    WHERE page = '/home'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched s
    INNER JOIN matched_keys k ON s.page = k.page AND s.event_type = k.event_type
),
with_dims_web_events AS (
    -- Attach sessions attributes
    SELECT
        e.*,
        s.device AS session_device,
        s.browser AS session_browser_2,
        s.country AS session_country_2
    FROM verified e
    INNER JOIN sessions s
        ON e.session_id = s.id
),
joined AS (
    -- Attach sessions attributes
    SELECT
        e.*,
        d.browser AS session_browser_3,
        d.country AS session_country_3,
        d.device AS session_device_2
    FROM with_dims_web_events e
    INNER JOIN sessions d
        ON e.session_id = d.id
),
enriched_web_events AS (
    -- Attach sessions attributes
    SELECT
        src.*,
        b.device AS session_device_3,
        b.browser AS session_browser_4
    FROM joined src
    LEFT JOIN sessions b
        ON src.session_id = b.id
)
SELECT
    id,
    session_id,
    event_type,
    page,
    duration_sec,
    page_group,
    page_filled,
    country,
    browser,
    CASE WHEN country RLIKE '^(SG|US)$' THEN 'GROUP_B' ELSE 'PRIORITY' END AS country_zone
FROM enriched_web_events
ORDER BY country NULLS FIRST;
