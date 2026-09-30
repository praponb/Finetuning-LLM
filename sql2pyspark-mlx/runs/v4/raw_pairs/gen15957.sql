WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        session_id,
        event_type,
        duration_sec,
        CASE WHEN page IN ('/cart', '/search') THEN 'Priority' ELSE 'Other' END page_group,
        DATEDIFF(CURRENT_DATE(), event_time) days_since_event_time
    FROM web_events
    WHERE event_time >= DATE '2024-05-13'
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(days_since_event_time) OVER (ORDER BY days_since_event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_days_since_event_time,
        FIRST_VALUE(duration_sec, TRUE) OVER (PARTITION BY event_type ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_duration_sec,
        SUM(duration_sec) OVER (PARTITION BY session_id, event_type ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_duration_sec,
        NTILE(10) OVER (PARTITION BY session_id ORDER BY days_since_event_time, id) days_since_event_time_bucket
    FROM filtered
),
valid AS (
    -- Keep recent, valid rows
    SELECT *, duration_sec / NULLIF(first_known_duration_sec, 0) duration_sec_per_first_known_duration_sec
    FROM with_windows
    WHERE days_since_event_time <= 42
),
enriched AS (
    -- Attach sessions attributes
    SELECT src.*, b.country
    FROM valid src
    LEFT JOIN sessions b
        ON src.session_id = b.id
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        UPPER(event_type) event_type_upper,
        COALESCE(country, 'DIRECT') country_filled
    FROM enriched
    WHERE running_duration_sec <= 1308
),
enriched_web_events AS (
    -- Attach sessions attributes
    SELECT a.*, p.device
    FROM cleaned a
    INNER JOIN sessions p
        ON a.session_id = p.id
),
filtered_web_events AS (
    -- Keep recent, valid rows
    SELECT
        *,
        SUBSTRING(event_type, 1, 3) event_type_prefix,
        INITCAP(TRIM(device)) device_title,
        COALESCE(device, 'UNKNOWN') device_clean
    FROM enriched_web_events
    WHERE running_duration_sec BETWEEN 1186 AND 1252
),
daily AS (
    -- Multi-level aggregation
    SELECT
        device,
        COUNT(*) FILTER (WHERE event_type = 'purchase') AS purchase_rows,
        COUNT(CASE WHEN country = 'US' THEN 1 END) AS us_count
    FROM filtered_web_events
    GROUP BY ROLLUP (device)
)
SELECT us_count
FROM daily
ORDER BY us_count DESC
