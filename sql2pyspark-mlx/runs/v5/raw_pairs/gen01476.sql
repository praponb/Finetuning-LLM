WITH valid AS (
    SELECT
        *,
        LOWER(event_type) AS event_type_lower,
        CONCAT(page, ' - ', event_type) AS page_event_type,
        COALESCE(page, 'DIRECT') AS page_clean
    FROM web_events
    WHERE event_time >= TO_DATE('2023-05-27')
      AND page IS NOT NULL
),
flagged AS (
    SELECT *, CASE WHEN event_time - LAG(event_time) OVER (PARTITION BY page ORDER BY event_time, id) > INTERVAL 90 DAYS THEN 1 ELSE 0 END AS new_group_flag
    FROM valid
),
islands AS (
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY page ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
enriched AS (
    SELECT
        id,
        page,
        event_type_lower,
        new_group_flag,
        LAG(session_no) OVER (PARTITION BY event_type ORDER BY event_time, id) AS prev_session_no,
        LEAD(session_no) OVER (PARTITION BY event_type ORDER BY event_time, id) AS next_session_no,
        LEAD(duration_sec) OVER (PARTITION BY event_type ORDER BY event_time, id) AS next_duration_sec,
        LAG(duration_sec) OVER (PARTITION BY event_type ORDER BY event_time, id) AS prev_duration_sec,
        ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY event_time, id) AS seq_no,
        FIRST_VALUE(duration_sec, TRUE) OVER (PARTITION BY page ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_duration_sec
    FROM islands
    WHERE event_type = 'click'
),
metrics AS (
    SELECT
        id,
        next_session_no,
        first_known_duration_sec,
        CUME_DIST() OVER (ORDER BY prev_session_no DESC NULLS FIRST) AS prev_session_no_cume_dist,
        AVG(first_known_duration_sec) OVER (PARTITION BY page ORDER BY prev_duration_sec, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS first_known_duration_sec_avg_6,
        NTILE(5) OVER (PARTITION BY page ORDER BY first_known_duration_sec, id) AS first_known_duration_sec_bucket
    FROM enriched
)
SELECT
    id,
    prev_session_no_cume_dist,
    first_known_duration_sec_bucket
FROM metrics
ORDER BY id DESC;
