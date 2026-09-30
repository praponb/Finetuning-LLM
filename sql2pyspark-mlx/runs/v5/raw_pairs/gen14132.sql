WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        session_id,
        event_type,
        page,
        LOWER(event_type) event_type_lower
    FROM web_events
    WHERE page IS DISTINCT FROM '/search'
      AND page <> '/product'
),
joined AS (
    -- Attach sessions attributes
    SELECT
        a.id,
        a.session_id,
        a.event_type,
        a.event_type_lower,
        d.country,
        d.device
    FROM valid a
    INNER JOIN sessions d
        ON a.session_id = d.id
),
joined_web_events AS (
    -- Attach sessions attributes
    SELECT
        a.id,
        a.session_id,
        a.device,
        s.country AS session_country,
        s.browser
    FROM joined a
    INNER JOIN sessions s
        ON a.session_id = s.id
    WHERE s.device NOT IN ('mobile', 'tablet')
),
kept_keys AS (
    -- Set difference on keys
    SELECT id, session_id
    FROM joined_web_events
    EXCEPT DISTINCT
    SELECT id, session_id
    FROM web_events
    WHERE event_time < '2023-05-01'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM joined_web_events s
    INNER JOIN kept_keys k ON s.id = k.id AND s.session_id = k.session_id
),
valid_web_events AS (
    -- Keep recent, valid rows
    SELECT
        *,
        INITCAP(TRIM(browser)) browser_title,
        LOWER(device) device_lower,
        COALESCE(session_country, 'NONE') resolved_session_country
    FROM verified
    WHERE browser = 'Edge'
)
-- Multi-level aggregation
SELECT
    browser,
    session_id,
    COUNT(*) AS cnt,
    COUNT(DISTINCT device) AS distinct_devices,
    COUNT(CASE WHEN browser = 'Firefox' THEN 1 END) AS firefox_count,
    GROUPING_ID() AS gid
FROM valid_web_events
GROUP BY GROUPING SETS ((browser, session_id), (browser), ())
HAVING firefox_count >= 1
ORDER BY session_id DESC NULLS FIRST;
