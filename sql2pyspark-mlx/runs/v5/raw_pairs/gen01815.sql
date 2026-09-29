SELECT event_cohort, first_event_time, web_event_count
FROM (
    SELECT CASE WHEN YEAR(event_time) < 2024 THEN 'before_2024' WHEN YEAR(event_time) = 2024 THEN '2024' ELSE 'after_2024' END AS event_cohort, MIN(event_time) AS first_event_time, COUNT(*) AS web_event_count
    FROM web_events
    GROUP BY CASE WHEN YEAR(event_time) < 2024 THEN 'before_2024' WHEN YEAR(event_time) = 2024 THEN '2024' ELSE 'after_2024' END
) AS x
WHERE web_event_count >= 4
