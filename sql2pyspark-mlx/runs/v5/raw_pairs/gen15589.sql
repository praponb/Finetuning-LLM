WITH kept_keys AS (
    -- Set intersection on keys
    SELECT event_type
    FROM web_events
    INTERSECT ALL
    SELECT event_type
    FROM web_events
    WHERE event_type = 'click'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM web_events s
    JOIN kept_keys k ON s.event_type = k.event_type
),
matched_keys AS (
    -- Set difference on keys
    SELECT page, id
    FROM verified
    EXCEPT
    SELECT page, id
    FROM web_events
    WHERE page = '/home'
),
verified_web_events AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM verified s
    JOIN matched_keys k ON s.page = k.page AND s.id = k.id
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        session_id,
        event_type,
        page,
        event_time,
        CONCAT(event_type, ' - ', page) AS event_type_page
    FROM verified_web_events
    WHERE page IS NOT NULL
      AND event_type IN ('purchase', 'add_to_cart')
),
with_dims AS (
    -- Attach sessions attributes
    SELECT
        a.session_id,
        a.event_type_page,
        p.browser,
        p.device
    FROM cleaned a
    LEFT JOIN sessions p
        ON a.session_id = p.id
),
enriched AS (
    -- Attach sessions attributes
    SELECT
        e.*,
        s.country,
        s.browser AS session_browser,
        s.device AS session_device
    FROM with_dims e
    LEFT JOIN sessions s
        ON e.session_id = s.id
),
with_dims_web_events AS (
    -- Attach sessions attributes
    SELECT
        src.session_id,
        src.country,
        src.session_browser,
        src.session_device,
        d.browser AS session_browser_2,
        d.country AS session_country
    FROM enriched src
    LEFT JOIN sessions d
        ON src.session_id = d.id
    WHERE d.customer_id IN (22, 25, 36)
),
joined AS (
    -- Attach sessions attributes
    SELECT s.*, b.country AS session_country_2
    FROM with_dims_web_events s
    JOIN sessions b
        ON s.session_id = b.id
)
-- Aggregate with conditional metrics
SELECT
    session_country_2,
    session_browser,
    COUNT_IF(session_device = 'desktop') AS desktop_events,
    SORT_ARRAY(COLLECT_SET(session_country_2)) AS session_country_2_list,
    COUNT(*) FILTER (WHERE country = 'TH') AS th_rows,
    COUNT_IF(session_browser = 'Edge') AS edge_events,
    GROUPING(session_country_2) AS grouping_id
FROM joined
GROUP BY GROUPING SETS ((session_country_2, session_browser), (session_country_2), ());
