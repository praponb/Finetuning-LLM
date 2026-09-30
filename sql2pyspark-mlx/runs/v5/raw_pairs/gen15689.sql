select we.id, we.event_type, we.event_time, we.session_id from web_events we where we.session_id not in (select id from sessions where browser in ('Firefox', 'Chrome'));
