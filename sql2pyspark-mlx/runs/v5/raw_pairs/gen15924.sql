select *
from (
    select
        status,
        customer_id,
        plan,
        monthly_fee
    from subscriptions
) src
pivot (
    avg(monthly_fee) as total,
    count(1) as n
    for plan in ('enterprise', 'standard')
);
