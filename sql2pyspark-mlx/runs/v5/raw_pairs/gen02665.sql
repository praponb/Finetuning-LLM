with base as (
    select *
    from orders
    where shipping_fee > 39
),
orders_stats as (
    select
        status,
        channel,
        count(case when channel = 'store' then 1 end) as store_count,
        max(discount) - min(discount) as discount_range
    from base
    group by status, channel
)
select
    status,
    channel,
    store_count,
    discount_range
from orders_stats
order by channel, status
limit 50;
