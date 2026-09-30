with filtered_orders as (
    select *
    from orders
    where customer_id = 26
),
orders_stats as (
    select channel, sort_array(collect_set(channel)) as channel_list
    from filtered_orders
    group by channel
)
select *
from orders_stats
order by channel;
