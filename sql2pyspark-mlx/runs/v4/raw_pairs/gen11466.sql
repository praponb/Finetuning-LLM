WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        event_type,
        event_time,
        duration_sec,
        LENGTH(page) AS page_length,
        LENGTH(page) AS page_length_2
    FROM web_events
    WHERE event_time >= TIMESTAMP '2023-07-08 21:00:00'
      AND page IS NOT NULL
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN event_time - LAG(event_time) OVER (PARTITION BY event_type ORDER BY event_time, id) > INTERVAL 90 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM filtered
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY event_type ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM with_gaps
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(3) OVER (PARTITION BY event_type ORDER BY island_id, id) AS island_id_bucket,
        LAST_VALUE(page_length_2, TRUE) OVER (PARTITION BY event_type ORDER BY page_length_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_page_length_2
    FROM islands
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY event_type ORDER BY island_id_bucket DESC NULLS FIRST) AS island_id_bucket_percent_rank,
        AVG(duration_sec) OVER (ORDER BY CAST(event_time AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS duration_sec_avg_7d,
        FIRST_VALUE(session_start_flag) IGNORE NULLS OVER w_running AS first_known_session_start_flag,
        FIRST_VALUE(island_id_bucket, TRUE) OVER (PARTITION BY event_type ORDER BY island_id_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_island_id_bucket,
        FIRST_VALUE(page_length_2, TRUE) OVER (PARTITION BY event_type ORDER BY session_start_flag, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_page_length_2
    FROM with_windows
    WINDOW w_running AS (PARTITION BY event_type ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
-- Window metrics over several different partitions
SELECT
    id,
    event_type,
    event_time,
    duration_sec,
    page_length,
    page_length_2,
    session_start_flag,
    island_id,
    island_id_bucket,
    last_known_page_length_2,
    island_id_bucket_percent_rank,
    duration_sec_avg_7d,
    first_known_session_start_flag,
    first_known_island_id_bucket,
    first_known_page_length_2,
    ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY island_id_bucket_percent_rank, id) AS event_seq,
    MAX(page_length) OVER (PARTITION BY event_type ORDER BY CAST(event_time AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS page_length_max_7d
FROM metrics
