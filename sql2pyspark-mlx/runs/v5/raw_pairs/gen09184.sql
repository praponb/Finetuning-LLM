with base as (
    -- Filter the raw rows and derive helper columns
    select *, substring(status, 1, 3) as status_prefix
    from orders
    where order_date > to_date('2023-08-31')
)
select channel, customer_id
from base
intersect all
select channel, customer_id
from orders
where shipping_fee < 13;
