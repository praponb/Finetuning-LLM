SELECT
    CASE WHEN duration_sec >= 530 THEN 'high' WHEN duration_sec >= 61 THEN 'mid' ELSE 'low' END AS duration_sec_bucket,
    page,
    YEAR(event_time) AS event_year,
    COUNT(CASE WHEN event_type = 'click' THEN 1 END) AS click_count,
    ROUND((100 * SUM(CASE WHEN page = '/home' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_home,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(event_type))) AS event_type_values
FROM web_events
GROUP BY duration_sec_bucket, page, event_year;
