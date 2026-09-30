WITH gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN event_time - LAG(event_time) OVER (PARTITION BY page ORDER BY event_time, id) > INTERVAL '30' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM web_events
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY page ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM gaps
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        STDDEV_SAMP(duration_sec) OVER (PARTITION BY session_id) session_id_stddev_samp_duration_sec,
        MAX(burst_id) OVER (PARTITION BY page ORDER BY CAST(event_time AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) burst_id_max_90d
    FROM sessionized
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        event_type,
        page,
        event_time,
        duration_sec,
        session_start_flag,
        session_id_stddev_samp_duration_sec,
        CASE WHEN duration_sec >= 503 THEN 'High' WHEN duration_sec >= 163 THEN 'Medium' ELSE 'Low' END duration_sec_level,
        CAST(session_start_flag AS STRING) session_start_flag_str
    FROM scored
    WHERE event_time > DATE '2023-09-03'
),
scored_web_events AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER w_grp duration_sec_rank,
        FIRST_VALUE(session_id_stddev_samp_duration_sec, TRUE) OVER (ORDER BY session_start_flag, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_session_id_stddev_samp_duration_sec
    FROM cleaned
    WINDOW w_grp AS (PARTITION BY event_type ORDER BY duration_sec DESC NULLS LAST)
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('QUARTER', event_time) AS quarter_start,
    DATE_TRUNC('WEEK', event_time) AS week_start,
    (UNIX_TIMESTAMP(MAX(event_time)) - UNIX_TIMESTAMP(MIN(event_time))) / 60 AS span_minutes,
    SUM(CASE WHEN event_type = 'click' THEN session_start_flag ELSE 0 END) AS click_session_start_flag,
    AVG(duration_sec_rank) AS avg_duration_sec_rank,
    MIN(event_time) AS started_at
FROM scored_web_events
GROUP BY GROUPING SETS ((DATE_TRUNC('QUARTER', event_time), DATE_TRUNC('WEEK', event_time)), (DATE_TRUNC('QUARTER', event_time)), ())
ORDER BY quarter_start DESC
