WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        device,
        browser,
        started_at,
        country,
        LENGTH(browser) AS browser_length,
        CASE WHEN browser = 'Edge' THEN 1 ELSE 0 END AS is_edge
    FROM sessions
    WHERE started_at >= TIMESTAMP '2024-03-21 13:00:00'
      AND browser IS NOT NULL
),
with_latest AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        latest.event_type,
        latest.page
    FROM filtered s
    JOIN LATERAL (
            SELECT c.event_type, c.page
            FROM web_events c
            WHERE c.session_id = s.id
              AND c.event_time <= s.started_at
            ORDER BY c.event_time DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        device,
        browser,
        started_at,
        browser_length,
        page,
        LENGTH(device) AS device_length,
        COALESCE(browser, 'NONE') AS browser_clean
    FROM with_latest
    WHERE event_type IN ('page_view', 'search', 'purchase')
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        MIN(browser_length) OVER (PARTITION BY browser, device) AS browser_min_browser_length,
        PERCENT_RANK() OVER (PARTITION BY page ORDER BY device_length) AS device_length_percent_rank
    FROM base
)
SELECT
    id,
    device,
    browser,
    browser_length,
    page,
    device_length,
    browser_clean,
    browser_min_browser_length
FROM metrics
ORDER BY id, browser_length DESC
