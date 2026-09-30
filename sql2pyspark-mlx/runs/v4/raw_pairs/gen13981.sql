select *
from (
    select
        order_id,
        status,
        amount
    from payments
) src
pivot (
    max(amount)
    for status in ('SUCCESS' as success, 'FAILED' as failed)
)
