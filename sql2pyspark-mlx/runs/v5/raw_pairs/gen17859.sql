select *
from (
    select
        method,
        order_id,
        status,
        amount
    from payments
) src
pivot (
    avg(amount) as total
    for status in ('SUCCESS' as success, 'REFUNDED' as refunded)
);
