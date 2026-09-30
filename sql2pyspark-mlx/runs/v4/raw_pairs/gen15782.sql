SELECT DATE_TRUNC('MONTH', s.started_at) started_month, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(we.page))) page_values, COUNT(*) num_web_events
FROM web_events we
INNER JOIN sessions s ON we.session_id = s.id
LEFT OUTER JOIN customers c ON c.id = s.customer_id
GROUP BY DATE_TRUNC('MONTH', s.started_at);
