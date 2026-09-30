with lines as (
    -- Unnest the array into one row per element
    select
        id,
        created_at,
        coupon_codes,
        key,
        v
    from carts
    lateral view outer explode(attributes) x as key, v
)
-- Grouped metrics
select
    date_trunc('day', created_at) as activity_day,
    count(*) filter (where key = 'gift') as gift_rows,
    min(created_at) as started_at,
    count_if(key = 'weight_kg') as weight_kg_events,
    (unix_timestamp(max(created_at)) - unix_timestamp(min(created_at))) / 60 as duration_min
from lines
group by date_trunc('day', created_at)
having count(*) filter (where key = 'gift') >= 1;
