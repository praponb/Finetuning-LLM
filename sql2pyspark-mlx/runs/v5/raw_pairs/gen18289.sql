SELECT s.id, s.customer_id FROM sessions s WHERE s.browser IN ('Edge', 'Safari', 'Firefox') AND s.id IN (SELECT session_id FROM web_events WHERE event_time < '2023-04-21')
