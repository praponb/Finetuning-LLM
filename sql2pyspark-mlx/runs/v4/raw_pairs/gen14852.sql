WITH filtered AS (
    SELECT
        id,
        customer_id,
        browser,
        started_at,
        country,
        SUBSTRING(browser, 1, 3) browser_prefix,
        CASE WHEN device IN ('mobile', 'tablet') THEN 'Priority' ELSE 'Other' END device_group,
        COALESCE(country, 'NONE') resolved_country
    FROM sessions
    WHERE started_at > TO_DATE('2023-03-21')
      AND country IS NOT NULL
),
metrics AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY browser ORDER BY started_at DESC, id) seq_no,
        FIRST_VALUE(country, TRUE) OVER (PARTITION BY customer_id ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_country
    FROM filtered
),
ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY started_at DESC, id) AS latest_rank
    FROM metrics
    QUALIFY latest_rank = 1
),
with_last AS (
    SELECT
        s.*,
        x.web_event_page,
        x.event_time
    FROM ranked s
    JOIN LATERAL (
            SELECT c.page AS web_event_page, c.event_time
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.event_time DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
joined AS (
    SELECT s.*, b.country AS customer_country
    FROM with_last s
    JOIN customers b
        ON s.customer_id = b.id
),
filtered_sessions AS (
    SELECT
        id,
        browser,
        started_at,
        country,
        browser_prefix,
        device_group,
        latest_rank,
        web_event_page,
        event_time,
        customer_country,
        CAST(event_time AS DATE) event_time_day,
        COALESCE(customer_country, 'UNKNOWN') resolved_customer_country
    FROM joined
    WHERE started_at >= TIMESTAMP '2024-01-30 15:00:00'
)
SELECT
    id,
    browser,
    country,
    browser_prefix,
    device_group,
    latest_rank,
    web_event_page,
    event_time,
    customer_country
FROM filtered_sessions
ORDER BY country, browser DESC
