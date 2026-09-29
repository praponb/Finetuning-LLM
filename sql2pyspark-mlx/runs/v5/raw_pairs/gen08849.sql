select we.id, we.duration_sec from web_events we where we.session_id not in (select id from sessions where to_date(started_at) >= '2024-11-01');
