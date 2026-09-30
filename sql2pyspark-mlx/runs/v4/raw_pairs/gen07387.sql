select hour(event_time) as event_hour, page, round(avg(duration_sec), 2) as avg_duration_sec
from web_events
where session_id in (3, 18, 28) or page is not null
group by 1, 2
order by avg_duration_sec desc
