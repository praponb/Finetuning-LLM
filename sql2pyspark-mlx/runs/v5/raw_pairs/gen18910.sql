WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CONCAT_WS('-', browser, device) AS browser_device_key
    FROM sessions
    WHERE customer_id IS NOT NULL
),
with_last AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.event_type,
        top1.duration_sec
    FROM filtered s
    LEFT JOIN LATERAL (
            SELECT c.event_type, c.duration_sec
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.duration_sec DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
latest_match AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.customer_id,
        s.browser,
        s.started_at,
        s.browser_device_key,
        latest.web_event_event_type,
        latest.event_time
    FROM with_last s
    LEFT JOIN LATERAL (
            SELECT c.event_type AS web_event_event_type, c.event_time
            FROM web_events c
            WHERE c.session_id = s.id
              AND c.event_time <= s.started_at
            ORDER BY c.event_time DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN event_time - LAG(event_time) OVER (PARTITION BY customer_id ORDER BY event_time, id) > INTERVAL 7 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM latest_match
),
sessions_sessions AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        customer_id,
        started_at,
        browser_device_key,
        web_event_event_type,
        event_time,
        SUM(is_new_session) OVER (PARTITION BY customer_id ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        TRIM(web_event_event_type) AS clean_web_event_event_type,
        COALESCE(web_event_event_type, 'N/A') AS web_event_event_type_clean
    FROM sessions_sessions
    WHERE web_event_event_type IS NOT NULL
)
-- Multi-level aggregation
SELECT
    web_event_event_type,
    DATE_TRUNC('WEEK', event_time) AS week_start,
    customer_id,
    SIZE(COLLECT_SET(web_event_event_type)) AS num_web_event_event_types,
    COUNT_IF(web_event_event_type = 'add_to_cart') AS add_to_cart_events,
    SUM(CASE WHEN web_event_event_type = 'add_to_cart' THEN island_id ELSE 0 END) AS add_to_cart_island_id,
    ROUND(AVG(island_id), 2) AS avg_island_id,
    COUNT_IF(web_event_event_type = 'page_view') AS page_view_events,
    COUNT(*) AS row_count,
    PERCENT_RANK() OVER (PARTITION BY web_event_event_type ORDER BY ROUND(AVG(island_id), 2) DESC) AS avg_island_id_percent_rank
FROM base
GROUP BY web_event_event_type, week_start, customer_id
ORDER BY page_view_events DESC NULLS LAST
