SELECT
    CASE WHEN s.browser IN ('Safari', 'Firefox') THEN 'priority' ELSE 'other' END AS browser_group,
    DATE_TRUNC('MONTH', s.started_at) AS started_month,
    COUNT(DISTINCT we.duration_sec) AS unique_pages,
    SUM(CASE WHEN we.page = '/home' THEN we.duration_sec ELSE 0 END) AS home_duration_sec
FROM web_events we
RIGHT OUTER JOIN sessions s ON we.session_id = s.id
GROUP BY CASE WHEN s.browser IN ('Safari', 'Firefox') THEN 'priority' ELSE 'other' END, DATE_TRUNC('MONTH', s.started_at);
