with cleaned as (
    select
        id,
        customer_id,
        store_id,
        order_date,
        status,
        channel,
        amount,
        coalesce(discount, 0) as discount_filled,
        coalesce(discount, 0) as discount_filled_2
    from orders
    where order_date > date '2023-03-25'
)
select a.*, d.store_name
from cleaned a
left join stores d
    on a.store_id = d.id;
