with valid as (
    -- Basic cleansing and defaults
    select
        *,
        concat(status, '/', account_type) as status_account_type,
        coalesce(status, 'OTHER') as status_clean
    from accounts
    where customer_id = 15
)
-- Non-equi join on a validity window
select
    s.*,
    sub.id as subscription_id,
    sub.plan,
    sub.monthly_fee
from valid s
left join subscriptions sub
    on s.customer_id = sub.customer_id
   and s.opened_date >= sub.start_date
   and s.opened_date <= date_add(sub.end_date, 3);
