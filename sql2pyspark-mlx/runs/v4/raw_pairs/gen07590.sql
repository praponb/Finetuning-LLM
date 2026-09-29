SELECT we.event_type, s.customer_id
FROM web_events we
INNER JOIN sessions s ON we.session_id = s.id
WHERE s.browser IN ('Chrome', 'Edge')
ORDER BY customer_id DESC, event_type DESC
