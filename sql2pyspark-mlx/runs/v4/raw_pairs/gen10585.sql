select *
from (
    select
        store_id,
        status,
        amount
    from orders
) src
pivot (
    sum(amount)
    for status in ('RETURNED', 'COMPLETED', 'PENDING')
);
