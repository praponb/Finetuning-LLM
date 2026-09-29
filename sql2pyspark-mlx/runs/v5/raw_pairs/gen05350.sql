with filtered_orders as (
    select *, coalesce(channel, 'N/A') as channel_filled
    from orders
    where channel = 'mobile' and store_id in (4, 9, 29, 40)
),
channel_summary as (
    select
        channel,
        customer_id,
        round(stddev(amount), 2) as stddev_amount,
        sum(discount) as total_discount
    from filtered_orders
    group by channel, customer_id
)
select *
from channel_summary
where total_discount >= 2.3
order by customer_id desc, total_discount desc;
