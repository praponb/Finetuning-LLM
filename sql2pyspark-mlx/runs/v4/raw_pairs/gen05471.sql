WITH with_prev AS (
    SELECT *, LAG(event_time) OVER (PARTITION BY event_type ORDER BY event_time ASC, id ASC) AS prev_event_time
    FROM web_events
),
with_gaps AS (
    SELECT *, CASE WHEN prev_event_time IS NULL OR event_time > prev_event_time + INTERVAL 14 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessions_web_events AS (
    SELECT *, SUM(is_new_session) OVER (PARTITION BY event_type ORDER BY event_time ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM with_gaps
)
SELECT
    DATE_TRUNC('YEAR', event_time) AS year_start,
    AVG(session_no) AS avg_session_no,
    SIZE(COLLECT_SET(page)) AS num_pages,
    COUNT(*) AS num_rows,
    GROUPING(DATE_TRUNC('YEAR', event_time)) AS gid
FROM sessions_web_events
GROUP BY ROLLUP (DATE_TRUNC('YEAR', event_time))
ORDER BY num_rows ASC NULLS FIRST
