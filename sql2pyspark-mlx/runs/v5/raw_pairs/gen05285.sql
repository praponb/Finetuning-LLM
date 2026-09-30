WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CASE WHEN event_type IN ('add_to_cart', 'click') THEN 'Group A' ELSE 'Other' END AS event_type_group,
        COALESCE(page, 'UNKNOWN') AS page_filled
    FROM web_events
    WHERE event_time > TO_DATE('2023-06-05')
      AND page IS NOT NULL
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM filtered
    QUALIFY ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY duration_sec DESC, id) = 1
),
enriched AS (
    -- Attach sessions attributes
    SELECT
        src.id,
        src.session_id,
        src.page,
        src.event_type_group,
        b.device,
        b.browser,
        b.country
    FROM deduped src
    LEFT JOIN sessions b
        ON src.session_id = b.id
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        session_id,
        page,
        event_type_group,
        browser,
        country,
        CONCAT_WS('-', page, browser) AS page_browser_key,
        INITCAP(TRIM(country)) AS country_title
    FROM enriched
    WHERE device IS NOT NULL
      AND country IS DISTINCT FROM 'UK'
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CASE WHEN country IN ('SG', 'US') THEN 'Core' ELSE 'Other' END AS country_group,
        COALESCE(country, 'NONE') AS country_filled
    FROM base
    WHERE browser IS NOT NULL
),
with_dims AS (
    -- Attach sessions attributes
    SELECT
        src.id,
        src.session_id,
        src.page,
        src.page_browser_key,
        src.country_title,
        src.country_group,
        d.country AS session_country,
        d.device
    FROM valid src
    LEFT JOIN sessions d
        ON src.session_id = d.id
),
base_web_events AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        session_id,
        page_browser_key,
        country_title,
        device,
        CONCAT(page, ', ', device) AS page_device
    FROM with_dims
    WHERE page IN ('/product', '/search', '/checkout')
)
SELECT
    s.id,
    s.session_id,
    s.country_title,
    s.device,
    CASE WHEN device RLIKE '^(desktop|tablet)$' THEN 'EXTENDED' WHEN device RLIKE '^(mobile)$' THEN 'PRIORITY' ELSE 'CORE' END AS device_zone,
    (SELECT MIN(x.duration_sec) FROM web_events x WHERE x.session_id = s.session_id) AS min_web_event_duration_sec
FROM base_web_events s
ORDER BY device DESC NULLS LAST;
