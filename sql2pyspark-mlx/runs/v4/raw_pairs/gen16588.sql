WITH valid AS (
    SELECT
        id,
        session_id,
        event_type,
        page,
        event_time,
        duration_sec,
        DAY(event_time) AS event_time_day
    FROM web_events
    WHERE duration_sec <= 454
),
flagged AS (
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY event_type ORDER BY event_time, id) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY event_type ORDER BY event_time, id) + INTERVAL 60 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM valid
),
islands AS (
    SELECT *, SUM(is_new_session) OVER (PARTITION BY event_type ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
)
SELECT
    DATE_TRUNC('YEAR', event_time) AS year_start,
    CASE WHEN duration_sec >= 383 THEN 'PREMIUM' ELSE 'STANDARD' END AS duration_sec_tier,
    COUNT_IF(event_type = 'search') AS search_events,
    ROUND(SUM(duration_sec) / NULLIF(COUNT(CASE WHEN event_type = 'page_view' THEN 1 END), 0), 2) AS duration_sec_per_page_view
FROM islands
GROUP BY DATE_TRUNC('YEAR', event_time), CASE WHEN duration_sec >= 383 THEN 'PREMIUM' ELSE 'STANDARD' END
HAVING duration_sec_per_page_view >= 0;
