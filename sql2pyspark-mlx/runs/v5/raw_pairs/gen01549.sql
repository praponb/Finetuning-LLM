select
    event_type,
    round(stddev(duration_sec), 2) as stddev_duration_sec,
    min(event_time) as min_event_time,
    round((100 * sum(case when event_type = 'click' then 1 else 0 end)) / count(*), 1) as pct_click
from web_events
group by event_type
