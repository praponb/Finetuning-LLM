select we.id, we.duration_sec, we.page, we.event_time from web_events we where we.session_id in (select id from sessions where country is null) order by we.duration_sec desc, we.page;
