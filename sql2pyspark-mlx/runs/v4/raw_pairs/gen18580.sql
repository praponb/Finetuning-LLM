select we.id, we.duration_sec, we.event_type, we.page from web_events we where we.session_id not in (select id from sessions where to_date(started_at) >= '2024-12-01') order by id desc;
