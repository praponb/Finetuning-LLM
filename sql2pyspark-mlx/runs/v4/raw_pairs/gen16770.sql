with joined as (
    -- Attach customers attributes
    select e.*, d.credit_limit
    from accounts e
    left join customers d
        on e.customer_id = d.id
)
select
    s.id,
    s.customer_id,
    s.account_type,
    s.balance,
    s.status,
    s.credit_limit,
    case when account_type rlike '^(LOAN|SAVINGS)$' then 'GROUP_B' when account_type rlike '^(CHECKING|CREDIT)$' then 'EXTENDED' else 'PRIORITY' end as account_type_zone,
    (select avg(x.monthly_fee) from subscriptions x where x.customer_id = s.customer_id) as avg_subscription_monthly_fee
from joined s
