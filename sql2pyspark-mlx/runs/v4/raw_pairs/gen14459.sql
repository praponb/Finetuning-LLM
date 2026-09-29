WITH base AS (
    SELECT
        id,
        customer_id,
        device,
        browser,
        started_at,
        UPPER(country) country_upper,
        CONCAT(device, ', ', browser) device_browser
    FROM sessions
    WHERE started_at > TIMESTAMP '2024-04-19 11:00:00'
),
with_latest AS (
    SELECT s.*, top1.web_event_page
    FROM base s
    JOIN LATERAL (
            SELECT c.page AS web_event_page
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.event_time DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
with_windows AS (
    SELECT
        *,
        FIRST_VALUE(web_event_page) IGNORE NULLS OVER (ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_web_event_page,
        ROW_NUMBER() OVER (PARTITION BY web_event_page, browser ORDER BY started_at DESC, id) seq_no,
        ROW_NUMBER() OVER (PARTITION BY device ORDER BY started_at, id) row_seq,
        LAST_VALUE(web_event_page, TRUE) OVER (PARTITION BY customer_id, browser ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_web_event_page,
        ROW_NUMBER() OVER (PARTITION BY web_event_page, device ORDER BY started_at DESC, id) event_seq
    FROM with_latest
),
with_last AS (
    SELECT
        s.*,
        top1.event_time,
        top1.event_type
    FROM with_windows s
    LEFT JOIN LATERAL (
            SELECT c.event_time, c.event_type
            FROM web_events c
            WHERE c.session_id = s.id
              AND c.event_time <= s.started_at
            ORDER BY c.event_time DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
ranked AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY browser ORDER BY event_seq DESC NULLS FIRST, id) AS latest_rank
        FROM with_last
    ) t
    WHERE latest_rank = 1
),
kept AS (
    SELECT s.*
    FROM ranked s
    WHERE EXISTS (
            SELECT 1
            FROM web_events c
            WHERE c.session_id = s.id
              AND c.event_time < s.started_at
        )
)
SELECT
    id,
    customer_id,
    device,
    browser,
    started_at,
    device_browser,
    first_known_web_event_page,
    seq_no,
    row_seq
FROM kept
ORDER BY row_seq NULLS FIRST
