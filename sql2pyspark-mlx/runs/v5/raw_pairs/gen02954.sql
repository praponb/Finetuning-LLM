with cleaned as (
    select
        *,
        amount * shipping_fee as amount_with_shipping,
        coalesce(channel, 'OTHER') as channel_filled
    from orders
    where shipping_fee between 14 and 21
)
select customer_id, channel
from cleaned
except
select customer_id, channel
from orders
where status = 'PENDING';
