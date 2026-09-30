with valid as (
    -- Basic cleansing and defaults
    select
        id,
        store_id,
        order_date,
        channel,
        amount,
        discount + shipping_fee as discount_plus_shipping_fee
    from orders
    where store_id is not null
      and channel is distinct from 'partner'
)
select channel, id
from valid
except
select channel, id
from orders
where shipping_fee between 35 and 36;
