SELECT web_events.duration_sec, sessions.country, sessions.browser, sessions.customer_id FROM web_events JOIN sessions ON web_events.session_id = sessions.id WHERE sessions.device = 'mobile';
