SELECT CASE WHEN YEAR(event_time) < 2024 THEN 'before_2024' WHEN YEAR(event_time) = 2024 THEN '2024' ELSE 'after_2024' END AS event_cohort, MAX(duration_sec) - MIN(duration_sec) AS duration_sec_range, SUM(CASE WHEN event_type = 'page_view' THEN 1 ELSE 0 END) AS page_view_count
FROM web_events
WHERE session_id = 34
GROUP BY CASE WHEN YEAR(event_time) < 2024 THEN 'before_2024' WHEN YEAR(event_time) = 2024 THEN '2024' ELSE 'after_2024' END
ORDER BY event_cohort;
