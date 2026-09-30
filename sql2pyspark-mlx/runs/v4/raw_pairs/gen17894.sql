SELECT CASE WHEN YEAR(event_time) < 2024 THEN 'before_2024' WHEN YEAR(event_time) = 2024 THEN '2024' ELSE 'after_2024' END AS event_cohort, SUM(duration_sec) AS total_duration_sec, DENSE_RANK() OVER (ORDER BY SUM(duration_sec) DESC) AS duration_sec_rank
FROM web_events
WHERE session_id = 31
GROUP BY CASE WHEN YEAR(event_time) < 2024 THEN 'before_2024' WHEN YEAR(event_time) = 2024 THEN '2024' ELSE 'after_2024' END
