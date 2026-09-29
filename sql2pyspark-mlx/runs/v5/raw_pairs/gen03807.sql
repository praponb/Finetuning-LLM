select
    we.id,
    we.duration_sec,
    we.session_id,
    we.event_time
from web_events we
where we.session_id in (select id from sessions where device = 'mobile')
order by event_time;
