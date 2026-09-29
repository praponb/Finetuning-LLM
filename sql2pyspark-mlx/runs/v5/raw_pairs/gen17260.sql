select we.id, we.duration_sec from web_events we where we.session_id in (select id from sessions where device in ('desktop', 'tablet', 'mobile'));
