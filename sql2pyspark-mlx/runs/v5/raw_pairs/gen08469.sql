select
    web_events.id,
    web_events.event_time,
    web_events.page,
    web_events.session_id
from web_events
where web_events.event_type != 'click' and web_events.session_id in (select id from sessions where started_at >= '2023-06-27');
