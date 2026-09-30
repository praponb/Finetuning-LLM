select event_month, max_duration_sec
from (
    select date_trunc('MONTH', event_time) as event_month, max(duration_sec) as max_duration_sec
    from web_events
    where session_id = 5
    group by date_trunc('MONTH', event_time)
) as x
where max_duration_sec >= 290
order by event_month;
