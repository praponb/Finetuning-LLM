WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, SUBSTRING(device, 1, 4) AS device_prefix
    FROM sessions
    WHERE country IS NOT NULL
),
with_last AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.device,
        s.device_prefix,
        top1.event_type,
        top1.web_event_event_time
    FROM valid s
    JOIN LATERAL (
            SELECT c.event_type, c.event_time AS web_event_event_time
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.duration_sec DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
with_latest AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.event_type,
        s.web_event_event_time,
        top1.web_event_event_type
    FROM with_last s
    LEFT JOIN LATERAL (
            SELECT c.event_type AS web_event_event_type
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.duration_sec DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
with_windows AS (
    -- Running and moving aggregates
    SELECT *, ROW_NUMBER() OVER (PARTITION BY web_event_event_type ORDER BY web_event_event_time DESC, id) AS event_seq
    FROM with_latest
    WHERE web_event_event_type = 'click'
),
totals AS (
    -- Grouped metrics
    SELECT
        DATE_TRUNC('YEAR', web_event_event_time) AS year_start,
        DATE_TRUNC('MONTH', web_event_event_time) AS activity_month,
        (UNIX_TIMESTAMP(MAX(web_event_event_time)) - UNIX_TIMESTAMP(MIN(web_event_event_time))) / 60 AS duration_min,
        MAX(event_seq) AS max_event_seq,
        COUNT(DISTINCT event_type) AS distinct_event_types
    FROM with_windows
    GROUP BY DATE_TRUNC('YEAR', web_event_event_time), DATE_TRUNC('MONTH', web_event_event_time)
)
SELECT
    year_start,
    activity_month,
    max_event_seq,
    distinct_event_types
FROM totals;
