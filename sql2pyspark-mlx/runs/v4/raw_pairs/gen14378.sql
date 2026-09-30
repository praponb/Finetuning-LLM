select *
from (
    select
        order_id,
        method,
        status,
        amount
    from payments
) src
pivot (
    max(amount)
    for status in ('SUCCESS', 'FAILED', 'REFUNDED')
);
