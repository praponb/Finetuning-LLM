WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        SUBSTRING(device, 1, 2) AS device_prefix,
        TO_DATE(started_at) AS started_at_date,
        COALESCE(country, 'DIRECT') AS country_clean
    FROM sessions
    WHERE started_at >= '2024-05-01'
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY browser ORDER BY started_at_date, id) AS dedup_rank
    FROM valid
    QUALIFY dedup_rank = 1
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        TO_DATE(started_at) AS started_at_date_2,
        CONCAT(browser, ' - ', device) AS browser_device
    FROM ranked
    WHERE started_at >= TO_DATE('2024-01-05')
      AND country IS NOT NULL
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM filtered
    QUALIFY RANK() OVER (PARTITION BY country ORDER BY started_at DESC NULLS LAST) = 1
),
with_latest AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        x.event_time,
        x.event_type
    FROM latest s
    JOIN LATERAL (
            SELECT c.event_time, c.event_type
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.duration_sec DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_latest s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events ch
            WHERE ch.session_id = s.id
              AND ch.event_type = 'purchase'
        )
),
kept_keys AS (
    -- Set difference on keys
    SELECT device
    FROM not_flagged
    EXCEPT DISTINCT
    SELECT device
    FROM sessions
    WHERE country IN ('DE', 'TH', 'UK')
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM not_flagged s
    JOIN kept_keys k ON s.device = k.device
),
daily AS (
    -- Grouped metrics
    SELECT
        DATE_TRUNC('MONTH', started_at_date_2) AS activity_month,
        event_type,
        SIZE(COLLECT_SET(device)) AS num_devices,
        MAX(started_at) AS last_seen,
        MIN(started_at) AS started_at,
        COUNT(DISTINCT browser) AS distinct_browsers,
        GROUPING(DATE_TRUNC('MONTH', started_at_date_2)) AS grouping_id
    FROM kept_rows
    GROUP BY GROUPING SETS ((DATE_TRUNC('MONTH', started_at_date_2), event_type), (DATE_TRUNC('MONTH', started_at_date_2)), ())
)
SELECT
    activity_month,
    num_devices,
    last_seen,
    started_at,
    distinct_browsers,
    grouping_id
FROM daily
ORDER BY activity_month
