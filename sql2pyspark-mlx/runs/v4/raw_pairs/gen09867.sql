WITH metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(duration_sec) OVER (PARTITION BY event_type, session_id ORDER BY event_time, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS duration_sec_avg_6,
        COUNT(duration_sec) OVER (PARTITION BY session_id, event_type) AS session_id_count_duration_sec,
        PERCENT_RANK() OVER (PARTITION BY event_type ORDER BY duration_sec DESC NULLS LAST) AS duration_sec_percent_rank,
        COUNT(duration_sec) OVER (PARTITION BY page ORDER BY CAST(event_time AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS duration_sec_count_7d,
        LEAD(duration_sec, 1) OVER (PARTITION BY session_id, page ORDER BY duration_sec, id) AS next_duration_sec
    FROM web_events
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN session_id_count_duration_sec >= 17 THEN 'LARGE' ELSE 'SMALL' END AS session_id_count_duration_sec_tier,
    AVG(session_id_count_duration_sec) AS avg_session_id_count_duration_sec,
    SUM(CASE WHEN event_type = 'add_to_cart' THEN session_id_count_duration_sec ELSE 0 END) AS add_to_cart_session_id_count_duration_sec,
    COUNT(DISTINCT session_id) AS distinct_session_ids
FROM metrics
GROUP BY CASE WHEN session_id_count_duration_sec >= 17 THEN 'LARGE' ELSE 'SMALL' END
