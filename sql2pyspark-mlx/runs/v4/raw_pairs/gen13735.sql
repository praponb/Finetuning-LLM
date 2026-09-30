WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        session_id,
        event_type,
        event_time,
        duration_sec,
        CONCAT(page, '/', event_type) AS page_event_type,
        CAST(event_time AS DATE) AS event_time_day,
        COALESCE(page, 'DIRECT') AS resolved_page
    FROM web_events
    WHERE page != '/home'
),
with_dims AS (
    -- Attach sessions attributes
    SELECT
        a.*,
        b.device,
        b.browser,
        b.country
    FROM base a
    INNER JOIN sessions b
        ON a.session_id = b.id
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, RANK() OVER (PARTITION BY event_type, session_id ORDER BY event_time NULLS LAST) AS latest_rank
    FROM with_dims
    QUALIFY latest_rank = 1
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('MONTH', event_time_day) AS activity_month,
    DATE_TRUNC('QUARTER', event_time) AS quarter_start,
    MIN(event_time) AS started_at,
    SUM(duration_sec) AS total_duration_sec
FROM latest
GROUP BY DATE_TRUNC('MONTH', event_time_day), DATE_TRUNC('QUARTER', event_time)
ORDER BY activity_month DESC NULLS LAST
