WITH with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(duration_sec) OVER (PARTITION BY event_type ORDER BY event_time, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS duration_sec_avg_2,
        SUM(duration_sec) OVER (PARTITION BY event_type, page) AS event_type_sum_duration_sec
    FROM web_events
)
-- Grouped metrics
SELECT
    CASE WHEN event_type_sum_duration_sec >= 2430 THEN 'LARGE' ELSE 'SMALL' END AS event_type_sum_duration_sec_tier,
    ROUND(SUM(duration_sec_avg_2) / NULLIF(COUNT(CASE WHEN page = '/home' THEN 1 END), 0), 2) AS duration_sec_avg_2_per_home,
    COUNT(CASE WHEN page = '/checkout' THEN 1 END) AS checkout_count,
    ROUND(AVG(duration_sec_avg_2), 2) AS avg_duration_sec_avg_2,
    MIN(event_time) AS first_event_time,
    SORT_ARRAY(COLLECT_SET(event_type)) AS event_type_list
FROM with_windows
GROUP BY CASE WHEN event_type_sum_duration_sec >= 2430 THEN 'LARGE' ELSE 'SMALL' END
HAVING duration_sec_avg_2_per_home > 0;
