select we.id, we.event_type, we.event_time, we.page from web_events we where we.session_id in (select id from sessions where to_date(started_at) >= '2024-06-12');
