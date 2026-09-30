WITH base AS (
    SELECT
        *,
        TRIM(device) AS clean_device,
        TRIM(device) AS clean_device_2,
        COALESCE(browser, 'N/A') AS browser_filled
    FROM sessions
    WHERE started_at > TIMESTAMP '2023-04-19 01:00:00'
),
with_latest AS (
    SELECT
        s.*,
        top1.page,
        top1.web_event_event_time
    FROM base s
    LEFT JOIN LATERAL (
            SELECT c.page, c.event_time AS web_event_event_time
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.event_time DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
with_last AS (
    SELECT s.*, x.web_event_page
    FROM with_latest s
    JOIN LATERAL (
            SELECT c.page AS web_event_page
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.duration_sec DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
enriched AS (
    SELECT
        a.*,
        d.city,
        d.email
    FROM with_last a
    JOIN customers d
        ON a.customer_id <=> d.id
),
with_last_sessions AS (
    SELECT
        s.*,
        x.web_event_duration_sec,
        x.web_event_event_type
    FROM enriched s
    JOIN LATERAL (
            SELECT c.duration_sec AS web_event_duration_sec, c.event_type AS web_event_event_type
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.event_time DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
SELECT
    c.segment,
    s.device,
    COUNT(*) AS num_rows,
    ROUND(SUM(s.web_event_duration_sec), 2) AS total_web_event_duration_sec
FROM with_last_sessions s
JOIN customers c ON s.customer_id = c.id
GROUP BY GROUPING SETS ((c.segment, s.device), (c.segment));
