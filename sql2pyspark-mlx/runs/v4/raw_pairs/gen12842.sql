select page, event_type_values, max_duration_sec
from (
    select page, concat_ws(', ', sort_array(collect_set(event_type))) as event_type_values, max(duration_sec) as max_duration_sec
    from web_events
    group by page
) as sub
where max_duration_sec > 150
order by max_duration_sec
