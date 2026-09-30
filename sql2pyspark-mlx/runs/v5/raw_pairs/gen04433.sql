SELECT we.page, s.country, c.credit_limit
FROM web_events we
JOIN sessions s ON we.session_id = s.id
LEFT JOIN customers c ON s.customer_id = c.id;
