select we.id, we.event_type from web_events we where we.session_id not in (select id from sessions where started_at >= '2024-02-08');
