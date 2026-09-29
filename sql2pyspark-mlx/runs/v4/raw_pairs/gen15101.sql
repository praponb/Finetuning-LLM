WITH valid AS (
    SELECT
        *,
        CONCAT_WS('/', page, event_type) AS page_event_type_key,
        INITCAP(TRIM(event_type)) AS event_type_title,
        COALESCE(page, 'OTHER') AS page_clean
    FROM web_events
    WHERE page IS NOT NULL
      AND event_type NOT IN ('purchase', 'add_to_cart', 'click')
),
deduped AS (
    SELECT *, RANK() OVER (PARTITION BY event_type ORDER BY duration_sec DESC NULLS FIRST) AS latest_rank
    FROM valid
    QUALIFY latest_rank = 1
),
gaps AS (
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY session_id ORDER BY event_time ASC, id ASC) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY session_id ORDER BY event_time ASC, id ASC) + INTERVAL 90 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM deduped
),
sessions_web_events AS (
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY session_id ORDER BY event_time ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM gaps
),
summary AS (
    SELECT
        session_no,
        CASE WHEN session_start_flag >= 1 THEN 'PREMIUM' ELSE 'STANDARD' END AS session_start_flag_tier,
        MAX(session_no) AS max_session_no,
        PERCENTILE_APPROX(session_start_flag, 0.95) AS session_start_flag_p95,
        ROUND(AVG(duration_sec), 2) AS avg_duration_sec,
        COUNT(*) FILTER (WHERE page = '/home') AS home_rows,
        SIZE(COLLECT_SET(page)) AS num_pages
    FROM sessions_web_events
    GROUP BY GROUPING SETS ((session_no, CASE WHEN session_start_flag >= 1 THEN 'PREMIUM' ELSE 'STANDARD' END), (session_no), ())
    HAVING avg_duration_sec < 0
)
SELECT
    session_no,
    max_session_no,
    session_start_flag_p95,
    home_rows,
    num_pages
FROM summary
ORDER BY max_session_no DESC NULLS FIRST, num_pages ASC
