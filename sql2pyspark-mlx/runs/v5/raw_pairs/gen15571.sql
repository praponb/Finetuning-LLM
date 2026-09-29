with filtered_orders as (
    select *
    from orders
    where store_id = 35 and customer_id in (4, 8, 21, 36)
),
channel_summary as (
    select channel, sum(case when status = 'COMPLETED' then amount else 0 end) as completed_amount, avg(shipping_fee) as avg_shipping_fee
    from filtered_orders
    group by channel
)
select channel, completed_amount, avg_shipping_fee
from channel_summary
