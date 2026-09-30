WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        COALESCE(country, 'N/A') AS country_filled,
        COALESCE(country, 'UNKNOWN') AS resolved_country
    FROM sessions
    WHERE browser IS NOT NULL
      AND started_at >= DATE '2023-02-16'
)
-- Latest matching web_event per row (LATERAL ... LIMIT 1)
SELECT
    s.id,
    s.customer_id,
    s.browser,
    s.started_at,
    s.country,
    s.resolved_country,
    top1.web_event_page,
    top1.web_event_event_time
FROM cleaned s
LEFT JOIN LATERAL (
        SELECT c.page AS web_event_page, c.event_time AS web_event_event_time
        FROM web_events c
        WHERE c.session_id = s.id
          AND c.event_time <= s.started_at
        ORDER BY c.event_time DESC, c.id DESC
        LIMIT 1
    ) top1 ON TRUE;
