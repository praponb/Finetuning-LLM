with base as (
    -- Keep recent, valid rows
    select
        id,
        customer_id,
        plan,
        start_date,
        monthly_fee,
        status,
        last_day(start_date) as start_date_month_end
    from subscriptions
    where customer_id is not null
)
-- Non-equi join on a validity window
select
    a.*,
    sub.id as subscription_id,
    sub.plan as subscription_plan,
    sub.monthly_fee as subscription_monthly_fee
from base a
inner join subscriptions sub
    on a.customer_id = sub.customer_id
   and a.start_date >= sub.start_date
   and a.start_date <= date_add(sub.end_date, 7);
