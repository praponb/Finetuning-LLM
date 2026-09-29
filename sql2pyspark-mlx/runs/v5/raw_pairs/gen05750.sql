SELECT DATE_TRUNC('MONTH', s.started_at) started_month, ROUND((100 * SUM(CASE WHEN we.page = '/checkout' THEN 1 ELSE 0 END)) / COUNT(*), 1) pct_checkout, SUM(CASE WHEN we.event_type = 'purchase' THEN we.duration_sec ELSE 0 END) purchase_duration_sec
FROM web_events we
INNER JOIN sessions s ON we.session_id = s.id
GROUP BY DATE_TRUNC('MONTH', s.started_at)
ORDER BY pct_checkout
