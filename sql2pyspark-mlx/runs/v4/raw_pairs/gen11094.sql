WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        LENGTH(page) AS page_length,
        ROUND(duration_sec * 100, 2) AS duration_sec_scaled
    FROM web_events
    WHERE page IS NOT NULL
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY session_id ORDER BY event_time, id) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY session_id ORDER BY event_time, id) + INTERVAL 60 DAYS THEN 1 ELSE 0 END AS new_group_flag
    FROM filtered
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY session_id ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id_2
    FROM flagged
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('WEEK', event_time) AS week_start,
    PERCENTILE_APPROX(duration_sec, ARRAY(0.25, 0.5, 0.75)) AS duration_sec_quartiles,
    ROUND(SUM(duration_sec_scaled) / NULLIF(COUNT(CASE WHEN page = '/cart' THEN 1 END), 0), 2) AS duration_sec_scaled_per_cart,
    SUM(CASE WHEN page = '/checkout' THEN page_length ELSE 0 END) AS checkout_page_length,
    MIN(event_time) AS first_seen,
    ROUND(AVG(duration_sec), 2) AS avg_duration_sec,
    BIT_AND(duration_sec) AS duration_sec_bit_and,
    GROUPING(DATE_TRUNC('WEEK', event_time)) AS gid
FROM sessionized
GROUP BY CUBE (DATE_TRUNC('WEEK', event_time))
