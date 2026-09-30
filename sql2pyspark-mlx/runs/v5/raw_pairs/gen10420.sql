WITH cleaned AS (
    SELECT
        *,
        CONCAT(page, ' ', event_type) AS page_event_type,
        COALESCE(page, 'N/A') AS page_filled
    FROM web_events
    WHERE page IS NOT DISTINCT FROM '/search'
      AND event_type IN ('search', 'click')
),
gaps AS (
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY event_type ORDER BY event_time, id) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY event_type ORDER BY event_time, id) + INTERVAL '90' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM cleaned
),
sessionized AS (
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY event_type ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM gaps
),
with_prev AS (
    SELECT *, LAG(event_time) OVER (PARTITION BY session_id ORDER BY event_time, id) AS prev_event_time
    FROM sessionized
),
flagged AS (
    SELECT *, CASE WHEN prev_event_time IS NULL OR event_time > prev_event_time + INTERVAL 60 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
islands AS (
    SELECT *, SUM(is_new_session) OVER (PARTITION BY session_id ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no_2
    FROM flagged
),
scored AS (
    SELECT
        *,
        COUNT(session_no) OVER (PARTITION BY event_type ORDER BY session_no_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_session_no,
        SUM(session_no) OVER (PARTITION BY event_type ORDER BY session_no_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_session_no,
        MAX(session_no) OVER (PARTITION BY event_type ORDER BY session_no_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_session_no,
        MIN(session_no) OVER (PARTITION BY event_type ORDER BY session_no_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_session_no
    FROM islands
)
SELECT
    s.id,
    s.session_id,
    s.page,
    s.event_time,
    s.duration_sec,
    s.page_event_type,
    s.page_filled,
    s.session_start_flag,
    s.prev_event_time,
    (SELECT MIN(w2.duration_sec) FROM web_events w2 WHERE w2.session_id = s.session_id) AS min_web_event_duration_sec
FROM scored s;
