select *
from (
    select
        plan,
        status,
        monthly_fee
    from subscriptions
) src
pivot (
    sum(monthly_fee) as value_sum
    for status in ('PAUSED', 'CANCELLED')
)
