with cleaned as (
    select
        id,
        order_id,
        method,
        amount,
        round(amount * 0.85, 2) as amount_scaled
    from payments
    where status <> 'SUCCESS'
)
select *
from (
    select
        order_id,
        method,
        amount_scaled
    from cleaned
) src
pivot (
    sum(amount_scaled) as amount,
    count(1) as n
    for method in ('cash', 'bank_transfer')
)
