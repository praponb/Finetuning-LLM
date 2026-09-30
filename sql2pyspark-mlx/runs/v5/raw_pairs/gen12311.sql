WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        INITCAP(TRIM(page)) AS page_title,
        COALESCE(page, 'DIRECT') AS resolved_page
    FROM web_events
    WHERE page IS NOT NULL
      AND page IN ('/product', '/search')
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY event_type ORDER BY event_time, id) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY event_type ORDER BY event_time, id) + INTERVAL 90 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM valid
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY event_type ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id_2
    FROM gaps
)
-- Multi-level aggregation
SELECT
    CASE WHEN session_start_flag >= 1 THEN 'HIGH' ELSE 'LOW' END AS session_start_flag_tier,
    DATE_TRUNC('QUARTER', event_time) AS quarter_start,
    CASE WHEN session_id_2 >= 9 THEN 'PREMIUM' ELSE 'STANDARD' END AS session_id_2_tier,
    ROUND(AVG(duration_sec), 2) AS avg_duration_sec,
    MAX(event_time) AS last_seen
FROM sessionized
GROUP BY CASE WHEN session_start_flag >= 1 THEN 'HIGH' ELSE 'LOW' END, DATE_TRUNC('QUARTER', event_time), CASE WHEN session_id_2 >= 9 THEN 'PREMIUM' ELSE 'STANDARD' END
HAVING ROUND(AVG(duration_sec), 2) > 0
ORDER BY avg_duration_sec DESC NULLS LAST;
