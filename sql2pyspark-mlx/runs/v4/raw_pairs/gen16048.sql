select we.id, we.duration_sec, we.event_time from web_events we where to_date(we.event_time) < '2024-02-01' and we.session_id not in (select id from sessions where device = 'desktop');
