SELECT
    CASE WHEN YEAR(s.started_at) < 2024 THEN 'before_2024' WHEN YEAR(s.started_at) = 2024 THEN '2024' ELSE 'after_2024' END AS started_cohort,
    s.device,
    COUNT(CASE WHEN we.event_type = 'purchase' THEN 1 END) AS purchase_count,
    COUNT(*) AS row_count
FROM web_events we
JOIN sessions s ON we.session_id = s.id
GROUP BY CASE WHEN YEAR(s.started_at) < 2024 THEN 'before_2024' WHEN YEAR(s.started_at) = 2024 THEN '2024' ELSE 'after_2024' END, s.device;
