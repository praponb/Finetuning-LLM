SELECT we.event_time, s.browser, c.signup_date, c.segment FROM web_events we LEFT JOIN sessions s ON we.session_id = s.id JOIN customers c ON s.customer_id = c.id;
