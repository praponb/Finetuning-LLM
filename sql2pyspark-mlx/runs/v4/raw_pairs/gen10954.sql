select
    to_date(event_time) as event_day,
    sum(duration_sec) as total_duration_sec,
    count(*) as cnt,
    round(sum(duration_sec) * 100.0 / sum(sum(duration_sec)) over (), 2) as pct_of_total
from web_events
group by to_date(event_time)
order by event_day, pct_of_total;
