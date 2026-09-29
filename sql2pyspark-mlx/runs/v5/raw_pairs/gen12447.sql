WITH cleaned AS (
    SELECT
        *,
        COALESCE(country, 'unassigned') AS country_filled,
        COALESCE(browser, 'N/A') AS browser_filled
    FROM sessions
    WHERE customer_id IS NOT NULL
)
SELECT
    s.id,
    s.customer_id,
    s.browser,
    s.country_filled,
    s.browser_filled,
    latest.web_event_event_time
FROM cleaned s
LEFT JOIN LATERAL (
        SELECT c.event_time AS web_event_event_time
        FROM web_events c
        WHERE c.session_id = s.id
          AND c.event_time <= s.started_at
        ORDER BY c.event_time DESC, c.id DESC
        LIMIT 1
    ) latest ON TRUE
