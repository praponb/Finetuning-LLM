WITH with_gaps AS (
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY session_id ORDER BY event_time, id) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY session_id ORDER BY event_time, id) + INTERVAL 30 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM web_events
),
islands AS (
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY session_id ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM with_gaps
)
SELECT
    page,
    session_id,
    MIN(event_time) AS first_event_time,
    BIT_AND(duration_sec) AS duration_sec_bit_and,
    COUNT(DISTINCT event_type) AS distinct_event_types,
    DENSE_RANK() OVER (PARTITION BY page ORDER BY BIT_AND(duration_sec) DESC) AS duration_sec_bit_and_dense_rank
FROM islands
GROUP BY 1, 2
