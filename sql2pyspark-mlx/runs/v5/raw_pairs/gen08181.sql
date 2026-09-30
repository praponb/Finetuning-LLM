select
    id,
    duration_sec,
    page,
    event_time
from web_events
where duration_sec >= (select max(duration_sec) from web_events where event_type = 'page_view')
order by 1 desc
limit 1
