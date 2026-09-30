SELECT
    p.browser,
    s.event_type,
    COUNT(*) AS num_rows,
    ROUND(SUM(s.duration_sec), 2) AS total_duration_sec,
    GROUPING_ID() AS grouping_level
FROM web_events s
LEFT JOIN sessions p ON s.session_id = p.id
GROUP BY GROUPING SETS ((p.browser, s.event_type), (p.browser), ())
ORDER BY browser NULLS FIRST
