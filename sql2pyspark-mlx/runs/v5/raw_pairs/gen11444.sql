select id, duration_sec, event_type
from web_events
where page = '/cart'
union
select id, duration_sec, event_type
from web_events
where page = '/search'
order by event_type;
