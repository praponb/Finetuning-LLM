WITH with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MIN(duration_sec) OVER w_recent last3_min_duration_sec,
        SUM(duration_sec) OVER w_recent last3_sum_duration_sec,
        MAX(duration_sec) OVER w_recent last3_max_duration_sec,
        AVG(duration_sec) OVER w_recent last3_avg_duration_sec,
        AVG(duration_sec) OVER w_part duration_sec_avg_3
    FROM web_events
    WINDOW w_recent AS (PARTITION BY page ORDER BY duration_sec, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), w_part AS (PARTITION BY session_id ORDER BY event_time, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
)
SELECT
    id,
    session_id,
    event_type,
    page,
    event_time,
    duration_sec,
    last3_min_duration_sec,
    last3_sum_duration_sec,
    last3_max_duration_sec
FROM with_windows
