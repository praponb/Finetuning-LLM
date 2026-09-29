SELECT
    page,
    SUM(duration_sec) total_duration_sec,
    COUNT(*) cnt,
    SUM(SUM(duration_sec)) OVER (ORDER BY page) AS cumulative_duration_sec
FROM web_events
WHERE event_time >= '2025-07-01'
GROUP BY page
ORDER BY cumulative_duration_sec DESC;
