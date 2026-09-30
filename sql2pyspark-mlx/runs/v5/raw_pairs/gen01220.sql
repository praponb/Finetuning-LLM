WITH gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN event_time - LAG(event_time) OVER (PARTITION BY page ORDER BY event_time, id) > INTERVAL 60 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM web_events
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY page ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM gaps
)
-- Grouped metrics
SELECT
    CASE WHEN duration_sec >= 374 THEN 'HIGH' ELSE 'LOW' END AS duration_sec_tier,
    MIN(event_time) AS started_at,
    BIT_AND(duration_sec) AS duration_sec_bit_and,
    COUNT(CASE WHEN event_type = 'search' THEN 1 END) AS search_count
FROM islands
GROUP BY ROLLUP (CASE WHEN duration_sec >= 374 THEN 'HIGH' ELSE 'LOW' END)
HAVING search_count >= 1
ORDER BY duration_sec_tier DESC;
