select
    event_type,
    page,
    round(stddev(duration_sec), 2) as stddev_duration_sec,
    min(event_time) as first_event_time,
    max(event_time) as latest_event_time
from web_events
where event_time < '2024-01-01' and hour(event_time) between 7 and 15 and page not in ('/product', '/checkout')
group by 1, 2
order by first_event_time
