WITH enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        event_type,
        LAST_VALUE(duration_sec) IGNORE NULLS OVER (ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_duration_sec,
        STDDEV_SAMP(duration_sec) OVER (PARTITION BY page) AS page_stddev_samp_duration_sec,
        LEAD(duration_sec, 1) OVER (PARTITION BY event_type ORDER BY duration_sec, id) AS next_duration_sec
    FROM web_events
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(last_known_duration_sec) OVER (PARTITION BY event_type ORDER BY next_duration_sec, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_max_last_known_duration_sec,
        AVG(last_known_duration_sec) OVER (PARTITION BY event_type ORDER BY next_duration_sec, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_avg_last_known_duration_sec,
        MIN(last_known_duration_sec) OVER (PARTITION BY event_type ORDER BY next_duration_sec, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_min_last_known_duration_sec
    FROM enriched
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        event_type,
        last_known_duration_sec,
        page_stddev_samp_duration_sec,
        next_duration_sec,
        last4_max_last_known_duration_sec,
        last4_min_last_known_duration_sec,
        last4_max_last_known_duration_sec - last4_avg_last_known_duration_sec AS last4_max_last_known_duration_sec_minus_last4_avg_last_known_duration_sec,
        CASE WHEN event_type IN ('search', 'purchase') THEN 'Group A' ELSE 'Other' END AS event_type_group
    FROM scored
    WHERE last4_max_last_known_duration_sec <= 300
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        STDDEV_SAMP(last4_max_last_known_duration_sec) OVER (PARTITION BY event_type) AS event_type_stddev_samp_last4_max_last_known_duration_sec,
        MAX(last4_max_last_known_duration_sec) OVER (PARTITION BY event_type) AS event_type_max_last4_max_last_known_duration_sec
    FROM valid
),
enriched_web_events AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        FIRST_VALUE(last4_max_last_known_duration_sec_minus_last4_avg_last_known_duration_sec, TRUE) OVER (PARTITION BY event_type ORDER BY last_known_duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_last4_max_last_known_duration_sec_minus_last4_avg_last_known_duration_sec,
        FIRST_VALUE(event_type_stddev_samp_last4_max_last_known_duration_sec, TRUE) OVER (ORDER BY event_type_stddev_samp_last4_max_last_known_duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_type_stddev_samp_last4_max_last_known_duration_sec,
        NTILE(4) OVER (PARTITION BY event_type ORDER BY event_type_max_last4_max_last_known_duration_sec, id) AS event_type_max_last4_max_last_known_duration_sec_bucket
    FROM with_windows
    WHERE page_stddev_samp_duration_sec <> 376
),
valid_web_events AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        event_type_stddev_samp_last4_max_last_known_duration_sec + event_type_max_last4_max_last_known_duration_sec AS event_type_stddev_samp_last4_max_last_known_duration_sec_plus_event_type_max_last4_max_last_known_duration_sec,
        last4_max_last_known_duration_sec + first_known_event_type_stddev_samp_last4_max_last_known_duration_sec AS last4_max_last_known_duration_sec_plus_first_known_event_type_stddev_samp_last4_max_last_known_duration_sec
    FROM enriched_web_events
    WHERE last4_max_last_known_duration_sec < 230
)
SELECT
    id,
    event_type,
    last_known_duration_sec,
    page_stddev_samp_duration_sec,
    next_duration_sec,
    last4_min_last_known_duration_sec,
    event_type_stddev_samp_last4_max_last_known_duration_sec,
    first_known_last4_max_last_known_duration_sec_minus_last4_avg_last_known_duration_sec,
    first_known_event_type_stddev_samp_last4_max_last_known_duration_sec,
    CASE WHEN event_type RLIKE '^(search|purchase)$' THEN 'EXTENDED' WHEN event_type RLIKE '^(page_view|click)$' THEN 'OTHER_ZONE' ELSE 'PRIORITY' END AS event_type_zone
FROM valid_web_events;
