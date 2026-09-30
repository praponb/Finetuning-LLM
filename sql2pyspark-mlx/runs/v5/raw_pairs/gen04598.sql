with valid as (
    -- Basic cleansing and defaults
    select
        *,
        case when monthly_fee >= 242 then 'High' when monthly_fee >= 76 then 'Medium' else 'Low' end as monthly_fee_band,
        round(monthly_fee * 1.1, 2) as monthly_fee_scaled
    from subscriptions
    where customer_id in (36, 38, 40)
),
matched as (
    -- Non-equi join on a validity window
    select
        s.*,
        sub.id as subscription_id,
        sub.plan as subscription_plan,
        sub.monthly_fee as subscription_monthly_fee
    from valid s
    left join subscriptions sub
        on s.customer_id = sub.customer_id
       and s.end_date >= sub.start_date
       and s.end_date < sub.end_date + interval '7' day
),
joined as (
    -- Attach customers attributes
    select
        a.*,
        c.name,
        c.segment
    from matched a
    left join customers c
        on a.customer_id <=> c.id
    where c.credit_limit < 15000
),
metrics as (
    -- Running and moving aggregates
    select
        id,
        customer_id,
        plan,
        end_date,
        monthly_fee_scaled,
        subscription_id,
        subscription_monthly_fee,
        name,
        lead(monthly_fee_scaled) over w_recent as next_monthly_fee_scaled,
        lag(monthly_fee_scaled) over w_recent as prev_monthly_fee_scaled,
        lead(monthly_fee) over w_recent as next_monthly_fee,
        lag(monthly_fee) over w_recent as prev_monthly_fee,
        row_number() over w_recent as event_seq
    from joined
    window w_recent as (partition by customer_id order by monthly_fee, id, subscription_id)
)
select
    s.id,
    s.customer_id,
    s.plan,
    s.end_date,
    s.monthly_fee_scaled,
    s.subscription_id,
    s.subscription_monthly_fee,
    s.name,
    s.next_monthly_fee_scaled,
    (select min(s2.monthly_fee) from subscriptions s2 where s2.customer_id = s.customer_id) as min_subscription_monthly_fee
from metrics s
order by plan nulls last, subscription_id desc
