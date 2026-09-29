WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CONCAT_WS('-', page, event_type) AS page_event_type_key,
        ROUND(duration_sec * 0.85, 2) AS adjusted_duration_sec
    FROM web_events
    WHERE page IS NOT NULL
),
enriched AS (
    -- Attach sessions attributes
    SELECT
        s.id,
        s.session_id,
        s.event_type,
        s.page,
        p2.browser,
        p2.country
    FROM base s
    LEFT JOIN sessions p2
        ON s.session_id = p2.id
    WHERE p2.device <> 'desktop'
),
joined AS (
    -- Attach sessions attributes
    SELECT
        e.*,
        p.country AS session_country,
        p.browser AS session_browser
    FROM enriched e
    LEFT JOIN sessions p
        ON e.session_id = p.id
),
enriched_web_events AS (
    -- Attach sessions attributes
    SELECT
        e.id,
        e.session_id,
        e.event_type,
        e.page,
        p.device,
        p.country AS session_country_2,
        p.browser AS session_browser_2
    FROM joined e
    LEFT JOIN sessions p
        ON e.session_id = p.id
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        session_id,
        event_type,
        page,
        device,
        session_country_2,
        SUBSTRING(device, 1, 3) AS device_prefix
    FROM enriched_web_events
    WHERE device <> 'desktop'
)
-- Grouped metrics
SELECT
    event_type,
    COUNT(CASE WHEN page = '/checkout' THEN 1 END) AS checkout_count,
    COUNT_IF(device = 'tablet') AS tablet_events,
    COUNT_IF(page = '/checkout') AS checkout_events,
    COUNT(*) FILTER (WHERE event_type = 'purchase') AS purchase_rows
FROM valid
GROUP BY event_type
HAVING tablet_events > 1
ORDER BY purchase_rows ASC NULLS FIRST;
