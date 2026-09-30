WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        device,
        browser,
        country,
        SUBSTRING(device, 1, 2) AS device_prefix,
        CASE WHEN browser = 'Edge' THEN 1 ELSE 0 END AS is_edge
    FROM sessions
    WHERE started_at >= DATE '2023-04-25'
      AND browser IS NOT NULL
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM base s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events x
            WHERE x.session_id = s.id
              AND x.duration_sec <> 469
        )
),
with_last AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.page
    FROM eligible s
    LEFT JOIN LATERAL (
            SELECT c.page
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.duration_sec DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
with_latest AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        latest.duration_sec,
        latest.web_event_page
    FROM with_last s
    LEFT JOIN LATERAL (
            SELECT c.duration_sec, c.page AS web_event_page
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.duration_sec DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
-- Join a dimension and aggregate
SELECT
    dim.segment,
    src.browser,
    COUNT(*) AS cnt,
    AVG(src.duration_sec) AS avg_duration_sec
FROM with_latest src
INNER JOIN customers dim ON src.customer_id = dim.id
GROUP BY dim.segment, src.browser
ORDER BY browser NULLS LAST;
