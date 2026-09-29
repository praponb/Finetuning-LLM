select *
from (
    select
        status,
        plan,
        monthly_fee
    from subscriptions
) src
pivot (
    avg(monthly_fee) as value_sum,
    count(1) as item_count
    for plan in ('standard' as standard, 'basic' as basic)
)
