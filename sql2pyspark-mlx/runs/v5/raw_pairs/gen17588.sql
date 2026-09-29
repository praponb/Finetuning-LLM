SELECT CASE WHEN YEAR(s.started_at) < 2024 THEN 'before_2024' WHEN YEAR(s.started_at) = 2024 THEN '2024' ELSE 'after_2024' END AS started_cohort, s.country, MAX(we.duration_sec) - MIN(we.duration_sec) AS duration_sec_range
FROM web_events we
INNER JOIN sessions s ON s.id = we.session_id
GROUP BY CASE WHEN YEAR(s.started_at) < 2024 THEN 'before_2024' WHEN YEAR(s.started_at) = 2024 THEN '2024' ELSE 'after_2024' END, s.country
ORDER BY s.country, 1;
