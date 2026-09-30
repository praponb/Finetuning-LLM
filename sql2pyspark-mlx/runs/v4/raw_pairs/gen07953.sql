select
    date_format(event_time, 'yyyy-MM') as event_month,
    sort_array(collect_set(event_type)) as event_type_list,
    max(duration_sec) as max_duration_sec,
    avg(duration_sec) as avg_duration_sec
from web_events
where duration_sec between 268 and 426 and event_type = 'search'
group by date_format(event_time, 'yyyy-MM')
