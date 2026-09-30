WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        device,
        browser,
        country,
        CASE WHEN browser = 'Firefox' THEN 1 ELSE 0 END AS is_firefox,
        CASE WHEN browser IN ('Safari', 'Firefox') THEN 'Tier 1' ELSE 'Other' END AS browser_group,
        COALESCE(browser, 'NONE') AS browser_clean
    FROM sessions
    WHERE customer_id IS NOT NULL
      AND browser IS DISTINCT FROM 'Firefox'
),
filtered_sessions AS (
    -- Keep recent, valid rows
    SELECT
        *,
        FLOOR(is_firefox) AS is_firefox_whole,
        CASE WHEN is_firefox >= 79 THEN 'High' WHEN is_firefox >= 23 THEN 'Medium' ELSE 'Low' END AS is_firefox_band,
        COALESCE(browser, 'UNKNOWN') AS resolved_browser
    FROM filtered
    WHERE browser IS NOT NULL
),
joined AS (
    -- Attach customers attributes
    SELECT
        e.id,
        e.customer_id,
        e.is_firefox,
        e.is_firefox_band,
        e.resolved_browser,
        b.city,
        b.name,
        b.country AS customer_country
    FROM filtered_sessions e
    LEFT JOIN customers b
        ON e.customer_id = b.id
),
latest_match AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.customer_id,
        s.is_firefox,
        s.is_firefox_band,
        s.resolved_browser,
        s.customer_country,
        x.web_event_event_type
    FROM joined s
    LEFT JOIN LATERAL (
            SELECT c.event_type AS web_event_event_type
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.event_time DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
with_latest AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.customer_id,
        s.is_firefox,
        s.is_firefox_band,
        s.customer_country,
        top1.event_type,
        top1.duration_sec
    FROM latest_match s
    LEFT JOIN LATERAL (
            SELECT c.event_type, c.duration_sec
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.event_time DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
SELECT
    id,
    is_firefox,
    is_firefox_band,
    customer_country,
    event_type
FROM with_latest;
