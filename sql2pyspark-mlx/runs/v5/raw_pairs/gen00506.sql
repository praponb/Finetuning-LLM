with cleaned as (
    select
        id,
        customer_id,
        store_id,
        order_date,
        status,
        channel,
        discount,
        case when channel = 'partner' then 1 else 0 end as is_partner
    from orders
    where order_date > date '2023-04-22'
      and store_id = 6
)
select channel
from cleaned
except distinct
select channel
from orders
where order_date > '2024-03-01';
