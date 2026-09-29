WITH base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CAST(event_time AS DATE) AS event_time_day,
        ROUND(duration_sec, 0) AS duration_sec_rounded
    FROM web_events
    WHERE event_type != 'click'
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM base
    QUALIFY ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY event_time ASC, id ASC) = 1
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        QUARTER(event_time) AS event_time_quarter,
        UPPER(event_type) AS event_type_upper,
        COALESCE(page, 'DIRECT') AS page_filled
    FROM deduped
    WHERE event_time > TIMESTAMP '2023-07-24 21:00:00'
),
enriched AS (
    -- Attach sessions attributes
    SELECT
        s.*,
        p.browser,
        p.country,
        p.device
    FROM filtered s
    INNER JOIN sessions p
        ON s.session_id = p.id
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY event_time_day ASC, id ASC) AS dedup_rank
    FROM enriched
    QUALIFY dedup_rank = 1
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY browser, event_type ORDER BY duration_sec ASC, id ASC) AS event_seq,
        LAG(event_time_quarter, 2) OVER (ORDER BY event_time_day ASC, id ASC) AS prev_event_time_quarter,
        NTILE(5) OVER (ORDER BY event_time_quarter ASC, id ASC) AS event_time_quarter_bucket,
        CUME_DIST() OVER (PARTITION BY browser, page ORDER BY duration_sec_rounded DESC NULLS LAST) AS duration_sec_rounded_cume_dist
    FROM latest
)
-- Grouped metrics
SELECT
    DATE_TRUNC('MONTH', event_time) AS activity_month,
    ROUND(AVG(duration_sec), 2) AS avg_duration_sec,
    SIZE(COLLECT_SET(device)) AS num_devices,
    SIZE(COLLECT_SET(browser)) AS num_browsers,
    APPROX_PERCENTILE(duration_sec, 0.95) AS duration_sec_p95,
    MAX(event_time) AS last_seen,
    GROUPING(DATE_TRUNC('MONTH', event_time)) AS grouping_id
FROM metrics
GROUP BY ROLLUP (DATE_TRUNC('MONTH', event_time));
