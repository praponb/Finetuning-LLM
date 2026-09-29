SELECT
    page,
    CASE WHEN YEAR(event_time) < 2024 THEN 'before_2024' WHEN YEAR(event_time) = 2024 THEN '2024' ELSE 'after_2024' END AS event_cohort,
    MIN(duration_sec) AS min_duration_sec,
    SUM(CASE WHEN page = '/checkout' THEN 1 ELSE 0 END) AS checkout_count,
    COUNT(*) AS total_count
FROM web_events
WHERE session_id IN (4, 18)
GROUP BY 1, 2
HAVING SUM(CASE WHEN event_type = 'purchase' THEN duration_sec ELSE 0 END) >= 24
ORDER BY page
