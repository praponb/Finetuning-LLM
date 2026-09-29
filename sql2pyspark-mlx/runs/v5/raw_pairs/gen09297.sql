select *
from (
    select
        customer_id,
        status,
        plan,
        monthly_fee
    from subscriptions
) src
pivot (
    avg(monthly_fee) as amount,
    count(1) as cnt
    for plan in ('standard', 'enterprise')
)
