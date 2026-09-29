with filtered as (
    -- Filter the raw rows and derive helper columns
    select *, concat(plan, '/', status) as plan_status
    from subscriptions
    where customer_id is not null
      and plan = 'standard'
),
enriched as (
    -- Attach customers attributes
    select
        s.*,
        c.age,
        c.signup_date
    from filtered s
    left join customers c
        on s.customer_id = c.id
),
matched as (
    -- Non-equi join on a validity window
    select
        src.*,
        sub.id as subscription_id,
        sub.plan as subscription_plan,
        sub.monthly_fee as subscription_monthly_fee
    from enriched src
    inner join subscriptions sub
        on src.customer_id = sub.customer_id
       and src.signup_date >= sub.start_date
       and src.signup_date < sub.end_date + interval 14 days
),
matched_subscriptions as (
    -- Non-equi join on a validity window
    select
        e.*,
        sub.id as subscription_id_2,
        sub.plan as subscription_plan_2,
        sub.monthly_fee as subscription_monthly_fee_2
    from matched e
    left join subscriptions sub
        on e.customer_id = sub.customer_id
       and e.end_date between sub.start_date and sub.end_date
)
select
    id,
    customer_id,
    plan,
    start_date,
    end_date,
    status,
    plan_status,
    subscription_id,
    subscription_plan
from matched_subscriptions
order by id desc, subscription_id desc;
