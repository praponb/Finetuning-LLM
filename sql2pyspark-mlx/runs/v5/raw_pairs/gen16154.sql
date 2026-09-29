WITH enriched AS (
    -- Attach sessions attributes
    SELECT
        a.id,
        a.session_id,
        a.event_type,
        a.page,
        d.country,
        d.device
    FROM web_events a
    INNER JOIN sessions d
        ON a.session_id = d.id
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT event_type, session_id
    FROM enriched
    INTERSECT
    SELECT event_type, session_id
    FROM web_events
    WHERE session_id IN (28, 29, 32, 36)
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched s
    INNER JOIN remaining_keys k ON s.event_type = k.event_type AND s.session_id = k.session_id
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CASE WHEN event_type IN ('purchase', 'click') THEN 'Core' ELSE 'Other' END AS event_type_group,
        CONCAT(page, ' ', event_type) AS page_event_type,
        COALESCE(country, 'N/A') AS resolved_country
    FROM kept_rows
    WHERE device IS NOT NULL
      AND device != 'desktop'
),
matched_keys AS (
    -- Set difference on keys
    SELECT page
    FROM cleaned
    EXCEPT
    SELECT page
    FROM web_events
    WHERE event_type = 'add_to_cart'
),
kept_rows_web_events AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM cleaned s
    INNER JOIN matched_keys k ON s.page = k.page
)
-- Multi-level aggregation
SELECT
    device,
    SIZE(COLLECT_SET(device)) AS num_devices,
    SIZE(COLLECT_SET(country)) AS num_countries,
    COUNT(*) FILTER (WHERE page = '/search') AS search_rows,
    COUNT(DISTINCT device) AS distinct_devices,
    COUNT(DISTINCT page) AS distinct_pages,
    COUNT(CASE WHEN event_type = 'add_to_cart' THEN 1 END) AS add_to_cart_count
FROM kept_rows_web_events
GROUP BY ROLLUP (device)
