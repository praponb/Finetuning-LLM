WITH base AS (
    SELECT *, INITCAP(TRIM(device)) AS device_title
    FROM sessions
    WHERE browser IS NOT NULL
      AND browser IS NOT DISTINCT FROM 'Firefox'
),
gaps AS (
    SELECT *, CASE WHEN LAG(started_at) OVER (PARTITION BY device ORDER BY started_at, id) IS NULL
                 OR started_at > LAG(started_at) OVER (PARTITION BY device ORDER BY started_at, id) + INTERVAL 14 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM base
),
sessionized AS (
    SELECT
        id,
        customer_id,
        device,
        browser,
        started_at,
        SUM(is_new_burst) OVER (PARTITION BY device ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM gaps
),
flagged AS (
    SELECT *, CASE WHEN LAG(started_at) OVER (PARTITION BY device ORDER BY started_at, id) IS NULL
                 OR started_at > LAG(started_at) OVER (PARTITION BY device ORDER BY started_at, id) + INTERVAL '7' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM sessionized
),
islands AS (
    SELECT
        id,
        customer_id,
        device,
        browser,
        started_at,
        session_id,
        SUM(session_start_flag) OVER (PARTITION BY device ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged
),
scored AS (
    SELECT
        id,
        customer_id,
        device,
        browser,
        started_at,
        island_id,
        LAG(island_id) OVER w_running AS prev_island_id,
        LAG(session_id) OVER w_running AS prev_session_id,
        LEAD(session_id) OVER w_running AS next_session_id,
        ROW_NUMBER() OVER w_running AS row_seq
    FROM islands
    WINDOW w_running AS (PARTITION BY device, browser ORDER BY island_id, id)
),
with_latest AS (
    SELECT
        s.id,
        s.prev_island_id,
        s.prev_session_id,
        s.next_session_id,
        x.event_type,
        x.event_time
    FROM scored s
    JOIN LATERAL (
            SELECT c.event_type, c.event_time
            FROM web_events c
            WHERE c.session_id = s.id
              AND c.event_time <= s.started_at
            ORDER BY c.event_time DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
enriched AS (
    SELECT
        *,
        LEAD(next_session_id) OVER (PARTITION BY event_type ORDER BY event_time, id) AS next_next_session_id,
        LAG(prev_session_id) OVER (PARTITION BY event_type ORDER BY event_time, id) AS prev_prev_session_id,
        LEAD(prev_session_id) OVER (PARTITION BY event_type ORDER BY event_time, id) AS next_prev_session_id,
        ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY event_time, id) AS seq_no,
        LEAD(prev_session_id, 1) OVER (PARTITION BY event_type ORDER BY prev_island_id, id) AS next_prev_session_id_2
    FROM with_latest
)
SELECT
    next_session_id,
    DATE_TRUNC('MONTH', event_time) AS activity_month,
    ROUND(SUM(prev_island_id) / NULLIF(COUNT(CASE WHEN event_type = 'add_to_cart' THEN 1 END), 0), 2) AS prev_island_id_per_add_to_cart,
    MIN(event_time) AS first_seen,
    MIN(event_time) AS started_at,
    COUNT(CASE WHEN event_type = 'purchase' THEN 1 END) AS purchase_count,
    (UNIX_TIMESTAMP(MAX(event_time)) - UNIX_TIMESTAMP(MIN(event_time))) / 60 AS duration_min,
    GROUPING_ID() AS gid
FROM enriched
GROUP BY CUBE (next_session_id, DATE_TRUNC('MONTH', event_time))
ORDER BY duration_min DESC NULLS FIRST
