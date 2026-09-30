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
    avg(monthly_fee)
    for plan in ('enterprise', 'standard', 'premium')
)
