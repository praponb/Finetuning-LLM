WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CASE WHEN device = 'desktop' THEN 1 ELSE 0 END AS is_desktop,
        CASE WHEN browser IN ('Safari', 'Edge') THEN 'Tier 1' ELSE 'Other' END AS browser_group,
        COALESCE(browser, 'UNKNOWN') AS browser_filled
    FROM sessions
    WHERE started_at > DATE '2024-03-24'
      AND browser IS NOT NULL
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        customer_id,
        device,
        browser,
        started_at,
        country,
        is_desktop,
        browser_group,
        browser_filled,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY started_at DESC, id) AS seq_no,
        LAG(is_desktop, 2) OVER (PARTITION BY device ORDER BY started_at, id) AS prev_is_desktop,
        NTILE(10) OVER (PARTITION BY country, device ORDER BY is_desktop DESC, id) AS is_desktop_bucket
    FROM base
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM scored s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events ch
            WHERE ch.session_id = s.id
              AND ch.event_time < s.started_at
        )
),
totals AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN prev_is_desktop >= 69 THEN 'HIGH' ELSE 'LOW' END AS prev_is_desktop_tier,
        COUNT_IF(device = 'mobile') AS mobile_events,
        AVG(seq_no) AS avg_seq_no,
        COUNT(*) FILTER (WHERE browser = 'Firefox') AS firefox_rows,
        COUNT(CASE WHEN country = 'DE' THEN 1 END) AS de_count,
        COUNT_IF(browser = 'Firefox') AS firefox_events
    FROM eligible
    GROUP BY CASE WHEN prev_is_desktop >= 69 THEN 'HIGH' ELSE 'LOW' END
)
SELECT
    prev_is_desktop_tier,
    mobile_events,
    avg_seq_no,
    firefox_rows,
    de_count,
    firefox_events
FROM totals
ORDER BY prev_is_desktop_tier
