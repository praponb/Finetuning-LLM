WITH latest AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY browser ORDER BY started_at DESC, id) AS dedup_rank
    FROM sessions
    QUALIFY dedup_rank = 1
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM latest s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events c
            WHERE c.session_id = s.id
              AND c.event_time < s.started_at
        )
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY device ORDER BY started_at, id) AS event_seq,
        ROW_NUMBER() OVER (PARTITION BY browser ORDER BY started_at DESC, id) AS row_seq,
        ROW_NUMBER() OVER w_grp AS seq_no
    FROM not_flagged
    WINDOW w_grp AS (PARTITION BY browser ORDER BY started_at, id)
),
base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        device,
        browser,
        started_at,
        country,
        event_seq,
        row_seq,
        seq_no,
        LENGTH(device) AS device_length
    FROM with_windows
    WHERE customer_id IS NOT NULL
      AND country IS DISTINCT FROM 'DE'
),
with_last AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.browser,
        s.started_at,
        s.country,
        s.event_seq,
        s.row_seq,
        s.device_length,
        latest.web_event_page,
        latest.web_event_duration_sec
    FROM base s
    LEFT JOIN LATERAL (
            SELECT c.page AS web_event_page, c.duration_sec AS web_event_duration_sec
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.duration_sec DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
prev_events AS (
    -- Previous event time per key
    SELECT *, LAG(started_at) OVER (PARTITION BY browser ORDER BY started_at, id) AS prev_started_at
    FROM with_last
),
gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_started_at IS NULL OR started_at > prev_started_at + INTERVAL 60 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM prev_events
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        browser,
        started_at,
        row_seq,
        device_length,
        SUM(session_start_flag) OVER (PARTITION BY browser ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM gaps
),
with_latest AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.started_at,
        s.device_length,
        x.event_time
    FROM sessionized s
    JOIN LATERAL (
            SELECT c.event_time
            FROM web_events c
            WHERE c.session_id = s.id
              AND c.event_time <= s.started_at
            ORDER BY c.event_time DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
-- Multi-level aggregation
SELECT
    CASE WHEN device_length >= 80 THEN 'HIGH' ELSE 'LOW' END AS device_length_tier,
    DATE_TRUNC('MONTH', event_time) AS activity_month,
    PERCENTILE_APPROX(device_length, ARRAY(0.25, 0.5, 0.75)) AS device_length_quartiles,
    (UNIX_TIMESTAMP(MAX(started_at)) - UNIX_TIMESTAMP(MIN(started_at))) / 60 AS duration_min,
    MAX(event_time) AS ended_at
FROM with_latest
GROUP BY CASE WHEN device_length >= 80 THEN 'HIGH' ELSE 'LOW' END, DATE_TRUNC('MONTH', event_time);
