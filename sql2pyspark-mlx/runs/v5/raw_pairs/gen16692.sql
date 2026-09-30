select *
from (
    select
        store_id,
        status,
        amount
    from orders
) src
pivot (
    avg(amount) as value_sum
    for status in ('SHIPPED', 'CANCELLED')
)
