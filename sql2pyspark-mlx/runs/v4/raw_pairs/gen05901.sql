select event_type
from web_events
where page = '/search'
union all
select event_type
from web_events
where session_id in (3, 20, 28, 31)
order by event_type desc;
