WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        INITCAP(TRIM(page)) AS page_title,
        DATE_TRUNC('month', event_time) AS event_time_period
    FROM web_events
    WHERE event_time > TO_TIMESTAMP('2023-08-13 19:00:00')
      AND page IS NOT NULL
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAST_VALUE(duration_sec, TRUE) OVER (PARTITION BY event_type ORDER BY duration_sec ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_duration_sec,
        SUM(duration_sec) OVER (PARTITION BY session_id, event_type ORDER BY CAST(event_time_period AS BIGINT) ASC RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) AS duration_sec_sum_90d
    FROM valid
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *, RANK() OVER (PARTITION BY page ORDER BY last_known_duration_sec ASC NULLS FIRST) AS row_num
    FROM enriched
    QUALIFY row_num = 1
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY event_type ORDER BY event_time ASC, id ASC) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY event_type ORDER BY event_time ASC, id ASC) + INTERVAL 14 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM ranked
),
sessions_web_events AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY event_type ORDER BY event_time ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id_2
    FROM with_gaps
)
-- Grouped metrics
SELECT
    CASE WHEN session_id_2 >= 8 THEN 'HIGH' ELSE 'LOW' END AS session_id_2_tier,
    DATE_TRUNC('YEAR', event_time) AS year_start,
    COUNT(DISTINCT event_type) AS distinct_event_types,
    APPROX_PERCENTILE(last_known_duration_sec, 0.95) AS last_known_duration_sec_p95,
    PERCENTILE_APPROX(duration_sec, ARRAY(0.25, 0.5, 0.75)) AS duration_sec_quartiles,
    GROUPING(CASE WHEN session_id_2 >= 8 THEN 'HIGH' ELSE 'LOW' END) AS grouping_level
FROM sessions_web_events
GROUP BY ROLLUP (CASE WHEN session_id_2 >= 8 THEN 'HIGH' ELSE 'LOW' END, DATE_TRUNC('YEAR', event_time))
ORDER BY distinct_event_types DESC NULLS LAST
