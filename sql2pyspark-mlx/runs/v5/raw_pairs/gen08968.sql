WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        page,
        event_time,
        duration_sec,
        COALESCE(duration_sec, 0) AS duration_sec_filled
    FROM web_events
    WHERE event_time > DATE '2023-04-11'
      AND page IS NOT NULL
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        page,
        event_time,
        duration_sec,
        ROW_NUMBER() OVER (PARTITION BY page ORDER BY duration_sec_filled DESC, id) AS seq_no,
        CUME_DIST() OVER (ORDER BY duration_sec NULLS FIRST) AS duration_sec_cume_dist
    FROM filtered
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        DATE_FORMAT(event_time, 'yyyy-MM') AS event_time_label,
        COALESCE(page, 'UNKNOWN') AS resolved_page
    FROM enriched
    WHERE event_time >= DATE '2023-03-27'
      AND duration_sec < 238
),
enriched_web_events AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(duration_sec) OVER (PARTITION BY page ORDER BY seq_no, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_duration_sec,
        COUNT(duration_sec) OVER (PARTITION BY page ORDER BY seq_no, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_duration_sec,
        AVG(duration_sec) OVER (PARTITION BY page ORDER BY seq_no, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_duration_sec,
        MIN(duration_sec) OVER (PARTITION BY page ORDER BY seq_no, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_duration_sec,
        ROW_NUMBER() OVER (PARTITION BY page ORDER BY duration_sec, id) AS row_seq
    FROM valid
),
filtered_web_events AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        page,
        event_time,
        duration_sec,
        duration_sec_cume_dist,
        resolved_page,
        running_count_duration_sec,
        running_avg_duration_sec,
        running_min_duration_sec,
        row_seq,
        duration_sec - running_sum_duration_sec AS duration_sec_minus_running_sum_duration_sec,
        LENGTH(page) AS page_length
    FROM enriched_web_events
    WHERE running_avg_duration_sec <= 400
),
with_prev AS (
    -- Previous event time per key
    SELECT *, LAG(event_time) OVER (PARTITION BY page ORDER BY event_time, id) AS prev_event_time
    FROM filtered_web_events
),
with_gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_event_time IS NULL OR event_time > prev_event_time + INTERVAL 90 DAYS THEN 1 ELSE 0 END AS new_group_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY page ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM with_gaps
),
prev_events AS (
    -- Previous event time per key
    SELECT *, LAG(event_time) OVER (PARTITION BY page ORDER BY event_time, id) AS prev_event_time_2
    FROM sessionized
),
gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_event_time_2 IS NULL OR event_time > prev_event_time_2 + INTERVAL 90 DAYS THEN 1 ELSE 0 END AS new_group_flag_2
    FROM prev_events
),
islands AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        page,
        event_time,
        running_avg_duration_sec,
        running_min_duration_sec,
        duration_sec_minus_running_sum_duration_sec,
        page_length,
        prev_event_time,
        new_group_flag,
        SUM(new_group_flag_2) OVER (PARTITION BY page ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM gaps
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('WEEK', event_time) AS week_start,
    DATE_TRUNC('MONTH', event_time) AS activity_month,
    MAX(new_group_flag) AS max_new_group_flag,
    COUNT_IF(page = '/search') AS search_events,
    ROUND(AVG(island_id), 2) AS avg_island_id,
    PERCENT_RANK() OVER (PARTITION BY DATE_TRUNC('WEEK', event_time) ORDER BY COUNT_IF(page = '/search') DESC) AS search_events_percent_rank
FROM islands
GROUP BY 1, 2
ORDER BY avg_island_id NULLS FIRST
