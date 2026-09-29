select
    id,
    store_id,
    shipping_fee,
    channel
from (
    select
        id,
        store_id,
        shipping_fee,
        channel,
        row_number() over (partition by store_id order by shipping_fee desc, id) as rn
    from orders
) as t
where rn <= 1
order by store_id desc;
