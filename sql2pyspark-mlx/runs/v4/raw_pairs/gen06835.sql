with cleaned as (
    -- Basic cleansing and defaults
    select *, round(monthly_fee * 0.9, 2) as adjusted_monthly_fee
    from subscriptions
    where end_date >= to_date('2023-08-01')
      and customer_id is not null
),
matched as (
    -- Non-equi join on a validity window
    select
        e.*,
        sub.id as subscription_id,
        sub.plan as subscription_plan,
        sub.monthly_fee as subscription_monthly_fee
    from cleaned e
    inner join subscriptions sub
        on e.customer_id = sub.customer_id
       and e.start_date >= sub.start_date
       and e.start_date < sub.end_date + interval 30 days
),
ranked as (
    -- Keep one row per key (latest first)
    select *, row_number() over (partition by subscription_plan order by end_date desc nulls last, id, subscription_id) as dedup_rank
    from matched
    qualify dedup_rank = 1
)
select
    id,
    customer_id,
    start_date,
    end_date,
    subscription_id,
    subscription_plan,
    subscription_monthly_fee
from ranked
order by start_date desc;
