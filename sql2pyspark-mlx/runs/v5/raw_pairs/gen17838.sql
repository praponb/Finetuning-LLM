with filtered as (
    select
        id,
        customer_id,
        store_id,
        order_date,
        status,
        channel,
        amount,
        discount,
        shipping_fee,
        cast(discount as int) as discount_int,
        substring(channel, 1, 4) as channel_prefix,
        coalesce(channel, 'OTHER') as channel_clean
    from orders
    where channel = 'partner'
)
select store_id, status
from filtered
intersect all
select store_id, status
from orders
where amount between 630 and 1530
