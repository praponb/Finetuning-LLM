WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        UPPER(event_type) event_type_upper,
        INITCAP(TRIM(page)) page_title
    FROM web_events
    WHERE event_time >= '2023-11-27'
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        FIRST_VALUE(duration_sec) IGNORE NULLS OVER (PARTITION BY session_id, page ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_duration_sec,
        NTILE(10) OVER (PARTITION BY event_type ORDER BY duration_sec, id) duration_sec_bucket
    FROM cleaned
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY event_type ORDER BY event_time, id) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY event_type ORDER BY event_time, id) + INTERVAL '7' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM metrics
),
sessions_web_events AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY event_type ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged
),
prev_events AS (
    -- Previous event time per key
    SELECT *, LAG(event_time) OVER (PARTITION BY page ORDER BY event_time, id) AS prev_event_time
    FROM sessions_web_events
),
with_gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_event_time IS NULL OR event_time > prev_event_time + INTERVAL 14 DAYS THEN 1 ELSE 0 END AS new_group_flag
    FROM prev_events
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY page ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM with_gaps
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM islands
    QUALIFY ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY prev_event_time NULLS FIRST, id) = 1
),
kept_keys AS (
    -- Set difference on keys
    SELECT page
    FROM latest
    EXCEPT ALL
    SELECT page
    FROM web_events
    WHERE TO_DATE(event_time) < '2023-07-01'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM latest s
    INNER JOIN kept_keys k ON s.page = k.page
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        CUME_DIST() OVER (PARTITION BY event_type ORDER BY is_new_burst DESC) is_new_burst_cume_dist,
        MAX(duration_sec) OVER (PARTITION BY event_type ORDER BY CAST(event_time AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) duration_sec_max_30d,
        STDDEV_SAMP(duration_sec_bucket) OVER (PARTITION BY event_type) event_type_stddev_samp_duration_sec_bucket
    FROM verified
)
-- Multi-level aggregation
SELECT
    CASE WHEN event_type_stddev_samp_duration_sec_bucket >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END AS event_type_stddev_samp_duration_sec_bucket_tier,
    DATE_TRUNC('WEEK', event_time) AS week_start,
    island_id,
    BIT_AND(duration_sec) AS duration_sec_bit_and,
    MIN(event_time) AS first_event_time,
    COUNT(DISTINCT page) AS distinct_pages
FROM enriched
GROUP BY CASE WHEN event_type_stddev_samp_duration_sec_bucket >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END, DATE_TRUNC('WEEK', event_time), island_id
ORDER BY week_start DESC NULLS FIRST;
