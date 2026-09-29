with lines as (
    select
        id,
        created_at,
        key,
        value
    from carts
    lateral view outer explode(attributes) t as key, value
)
select
    date_trunc('hour', created_at) as hour_window,
    max(created_at) as ended_at,
    size(collect_set(key)) as num_keys,
    count(*) as num_rows,
    count(case when key = 'device' then 1 end) as device_count,
    count_if(key = 'device') as device_events,
    sort_array(collect_set(key)) as key_list
from lines
group by date_trunc('hour', created_at)
order by num_rows
