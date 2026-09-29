with lines as (
    select
        id,
        created_at,
        attr_key,
        attr_value
    from carts
    lateral view explode(attributes) li as attr_key, attr_value
)
select
    date_trunc('day', created_at) as activity_day,
    min(created_at) as first_created_at,
    count_if(attr_key = 'device') as device_events,
    count(distinct attr_key) as distinct_attr_keys,
    size(collect_set(attr_key)) as num_attr_keys,
    count(case when attr_key = 'utm_source' then 1 end) as utm_source_count
from lines
group by date_trunc('day', created_at)
order by utm_source_count desc nulls first;
