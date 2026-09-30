select
    id,
    page,
    duration_sec,
    ceil(duration_sec) as duration_sec_whole
from web_events
where page is not null and event_time >= '2024-03-01'
order by duration_sec_whole desc, duration_sec, id
limit 20;
