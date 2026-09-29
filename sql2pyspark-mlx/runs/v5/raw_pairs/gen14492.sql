WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        session_id,
        event_type,
        page,
        event_time,
        duration_sec,
        COALESCE(duration_sec, 0) duration_sec_filled
    FROM web_events
    WHERE page IN ('/checkout', '/product', '/cart')
),
enriched AS (
    -- Attach sessions attributes
    SELECT
        a.id,
        a.session_id,
        a.event_type,
        p.browser,
        p.device,
        p.country
    FROM base a
    INNER JOIN sessions p
        ON a.session_id = p.id
    WHERE p.browser != 'Firefox'
),
base_web_events AS (
    -- Keep recent, valid rows
    SELECT
        id,
        session_id,
        event_type,
        browser,
        device,
        country,
        UPPER(event_type) event_type_upper,
        COALESCE(browser, 'UNKNOWN') resolved_browser
    FROM enriched
    WHERE device IS DISTINCT FROM 'tablet'
),
enriched_web_events AS (
    -- Attach sessions attributes
    SELECT
        src.*,
        d.browser AS session_browser,
        d.device AS session_device
    FROM base_web_events src
    LEFT JOIN sessions d
        ON src.session_id = d.id
),
joined AS (
    -- Attach sessions attributes
    SELECT
        s.*,
        b.country AS session_country,
        b.device AS session_device_2,
        b.browser AS session_browser_2
    FROM enriched_web_events s
    LEFT JOIN sessions b
        ON s.session_id = b.id
),
with_dims AS (
    -- Attach sessions attributes
    SELECT
        src.id,
        src.session_id,
        src.browser,
        src.country,
        src.resolved_browser,
        src.session_country,
        src.session_device_2,
        d.country AS session_country_2
    FROM joined src
    LEFT JOIN sessions d
        ON src.session_id = d.id
),
joined_web_events AS (
    -- Attach sessions attributes
    SELECT s.*, p2.country AS session_country_3
    FROM with_dims s
    LEFT JOIN sessions p2
        ON s.session_id = p2.id
)
-- Grouped metrics
SELECT
    country,
    SORT_ARRAY(COLLECT_SET(session_country_3)) AS session_country_3_list,
    COUNT(DISTINCT id) AS distinct_ids
FROM joined_web_events
GROUP BY country
ORDER BY country DESC
