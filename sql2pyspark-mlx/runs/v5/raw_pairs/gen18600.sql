WITH enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        session_id,
        event_type,
        page,
        event_time,
        duration_sec,
        AVG(duration_sec) OVER w_hist AS running_avg_duration_sec,
        COUNT(duration_sec) OVER w_hist AS running_count_duration_sec,
        SUM(duration_sec) OVER w_hist AS running_sum_duration_sec,
        MIN(duration_sec) OVER w_hist AS running_min_duration_sec
    FROM web_events
    WHERE page IS NOT NULL
    WINDOW w_hist AS (PARTITION BY session_id, event_type ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        session_id,
        event_type,
        event_time,
        duration_sec,
        running_avg_duration_sec,
        running_sum_duration_sec,
        running_min_duration_sec,
        running_count_duration_sec * duration_sec AS running_count_duration_sec_times_duration_sec
    FROM enriched
    WHERE event_time > DATE '2023-05-13'
      AND session_id IN (18, 31, 34, 35)
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN event_time - LAG(event_time) OVER (PARTITION BY event_type ORDER BY event_time, id) > INTERVAL 90 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM filtered
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY event_type ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM gaps
),
filtered_web_events AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        session_id,
        event_type,
        event_time,
        duration_sec,
        running_avg_duration_sec,
        running_sum_duration_sec,
        running_count_duration_sec_times_duration_sec,
        is_new_burst,
        burst_id,
        TO_DATE(event_time) AS event_time_date
    FROM sessionized
    WHERE event_time >= TIMESTAMP '2023-05-16 06:00:00'
)
-- Join a dimension and aggregate
SELECT
    d.browser,
    e.event_type,
    COUNT(*) AS num_rows,
    MAX(e.running_avg_duration_sec) AS max_running_avg_duration_sec,
    ROUND(SUM(e.running_count_duration_sec_times_duration_sec), 2) AS total_running_count_duration_sec_times_duration_sec
FROM filtered_web_events e
LEFT JOIN sessions d ON e.session_id = d.id
GROUP BY d.browser, e.event_type
