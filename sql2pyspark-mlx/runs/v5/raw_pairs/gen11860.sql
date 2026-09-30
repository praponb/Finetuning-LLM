WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        page,
        event_time,
        CAST(duration_sec AS STRING) AS duration_sec_num,
        ROUND(duration_sec * 1.07, 2) AS duration_sec_adjusted,
        COALESCE(page, 'N/A') AS page_filled
    FROM web_events
    WHERE event_time > TIMESTAMP '2023-07-11 20:00:00'
      AND page IS NOT NULL
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        FIRST_VALUE(page) IGNORE NULLS OVER (PARTITION BY page ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_page,
        AVG(duration_sec_adjusted) OVER (PARTITION BY page ORDER BY event_time, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS duration_sec_adjusted_avg_2
    FROM cleaned
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN event_time - LAG(event_time) OVER (PARTITION BY page ORDER BY event_time, id) > INTERVAL 30 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM with_windows
),
islands AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        page,
        event_time,
        duration_sec_adjusted,
        page_filled,
        first_known_page,
        SUM(is_new_session) OVER (PARTITION BY page ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
with_prev AS (
    -- Previous event time per key
    SELECT *, LAG(event_time) OVER (PARTITION BY page ORDER BY event_time, id) AS prev_event_time
    FROM islands
),
flagged_2 AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_event_time IS NULL OR event_time > prev_event_time + INTERVAL 14 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM with_prev
),
islands_web_events AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY page ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id_2
    FROM flagged_2
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('QUARTER', event_time) AS quarter_start,
    page,
    DATE_TRUNC('YEAR', prev_event_time) AS year_start,
    APPROX_PERCENTILE(session_start_flag, 0.95) AS session_start_flag_p95,
    ROUND(AVG(session_id), 2) AS avg_session_id,
    COUNT(CASE WHEN page = '/home' THEN 1 END) AS home_count,
    SORT_ARRAY(COLLECT_SET(page)) AS page_list,
    RANK() OVER (PARTITION BY DATE_TRUNC('QUARTER', event_time) ORDER BY ROUND(AVG(session_id), 2) DESC) AS avg_session_id_rank
FROM islands_web_events
GROUP BY DATE_TRUNC('QUARTER', event_time), page, DATE_TRUNC('YEAR', prev_event_time)
ORDER BY avg_session_id DESC NULLS FIRST;
