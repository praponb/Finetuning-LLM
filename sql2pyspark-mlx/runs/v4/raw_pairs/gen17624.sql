SELECT
    session_id,
    CASE WHEN duration_sec >= 457 THEN 'HIGH' ELSE 'LOW' END AS duration_sec_tier,
    DATE_TRUNC('MONTH', event_time) AS activity_month,
    STDDEV_SAMP(duration_sec) AS duration_sec_stddev,
    COUNT(*) FILTER (WHERE event_type = 'add_to_cart') AS add_to_cart_rows,
    COUNT(CASE WHEN event_type = 'click' THEN 1 END) AS click_count,
    SUM(duration_sec) FILTER (WHERE event_type = 'search') AS search_total_duration_sec
FROM web_events
GROUP BY 1, 2, 3
ORDER BY duration_sec_tier
