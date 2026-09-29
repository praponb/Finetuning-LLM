select
    id,
    store_id,
    shipping_fee,
    amount
from (
    select *, row_number() over (partition by store_id order by shipping_fee desc, id) pos
    from orders
) t
where pos <= 3
order by amount desc
