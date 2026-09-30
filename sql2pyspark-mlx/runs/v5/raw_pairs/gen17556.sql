WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        session_id,
        event_type,
        duration_sec,
        COALESCE(duration_sec, 0) AS duration_sec_filled,
        UPPER(event_type) AS event_type_upper,
        COALESCE(page, 'N/A') AS page_clean
    FROM web_events
    WHERE event_time >= DATE '2023-05-30'
),
enriched AS (
    -- Attach sessions attributes
    SELECT
        s.*,
        p.browser,
        p.country,
        p.device
    FROM valid s
    INNER JOIN sessions p
        ON s.session_id = p.id
),
joined AS (
    -- Attach sessions attributes
    SELECT
        s.id,
        s.session_id,
        s.duration_sec,
        s.duration_sec_filled,
        s.event_type_upper,
        s.page_clean,
        s.browser,
        s.country,
        d.device AS session_device,
        d.browser AS session_browser
    FROM enriched s
    LEFT JOIN sessions d
        ON s.session_id = d.id
),
enriched_web_events AS (
    -- Attach sessions attributes
    SELECT
        s.*,
        p.browser AS session_browser_2,
        p.country AS session_country,
        p.device
    FROM joined s
    LEFT JOIN sessions p
        ON s.session_id = p.id
)
SELECT
    s.id,
    s.session_id,
    s.duration_sec,
    s.duration_sec_filled,
    s.event_type_upper,
    s.country,
    s.session_device,
    s.session_browser_2,
    s.session_country,
    CASE WHEN country RLIKE '^(TH|DE)$' THEN 'GROUP_B' WHEN country RLIKE '^(US|SG)$' THEN 'PRIORITY' ELSE 'GROUP_A' END AS country_zone,
    (SELECT MAX(x.duration_sec) FROM web_events x WHERE x.session_id = s.session_id) AS max_web_event_duration_sec
FROM enriched_web_events s
ORDER BY session_browser_2 DESC;
