with recent_orders as (
    select *
    from orders
    where customer_id is not null
),
orders_stats as (
    select channel, concat_ws(', ', sort_array(collect_set(status))) as status_values
    from recent_orders
    group by channel
)
select *
from orders_stats;
