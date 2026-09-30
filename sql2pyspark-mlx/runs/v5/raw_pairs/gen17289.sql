WITH valid AS (
    SELECT
        id,
        session_id,
        event_type,
        event_time,
        duration_sec,
        CAST(event_time AS DATE) AS event_time_day,
        COALESCE(page, 'OTHER') AS resolved_page
    FROM web_events
    WHERE session_id = 28
)
SELECT
    id,
    session_id,
    event_time_day,
    SUM(duration_sec) OVER w_recent AS duration_sec_sum_30d,
    LAG(duration_sec, 1) OVER (PARTITION BY event_type, session_id ORDER BY event_time_day, id) AS prev_duration_sec,
    FIRST_VALUE(duration_sec, TRUE) OVER (PARTITION BY session_id ORDER BY event_time_day, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_duration_sec,
    ROW_NUMBER() OVER w_grp AS seq_no,
    SUM(duration_sec) OVER w_running AS cumulative_duration_sec
FROM valid
WINDOW w_recent AS (ORDER BY CAST(event_time AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW), w_grp AS (PARTITION BY event_type, session_id ORDER BY event_time DESC, id), w_running AS (ORDER BY event_time_day, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
