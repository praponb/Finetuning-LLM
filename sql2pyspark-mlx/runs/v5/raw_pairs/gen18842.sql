with exploded as (
    select
        id,
        customer_id,
        created_at,
        tag
    from carts
    lateral view explode(coupon_codes) e as tag
)
select
    date_trunc('day', created_at) as activity_day,
    date_trunc('hour', created_at) as hour_window,
    count_if(tag = 'WELCOME5') as welcome5_events,
    (unix_timestamp(max(created_at)) - unix_timestamp(min(created_at))) / 60 as duration_min,
    (unix_timestamp(max(created_at)) - unix_timestamp(min(created_at))) / 60 as span_minutes,
    count_if(tag = 'WELCOME5') as welcome5_events_2
from exploded
group by grouping sets ((date_trunc('day', created_at), date_trunc('hour', created_at)), (date_trunc('day', created_at)), ())
having duration_min > 0
order by hour_window desc nulls first;
