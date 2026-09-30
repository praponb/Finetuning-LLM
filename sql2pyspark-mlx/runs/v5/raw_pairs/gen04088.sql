with cleaned as (
    -- Filter the raw rows and derive helper columns
    select *, date_add(opened_date, 21) as opened_date_plus_21d
    from accounts
    where customer_id in (2, 7, 30)
),
attributed as (
    -- Non-equi join on a validity window
    select
        e.*,
        sub.id as subscription_id,
        sub.plan,
        sub.monthly_fee
    from cleaned e
    left join subscriptions sub
        on e.customer_id = sub.customer_id
       and e.opened_date_plus_21d between sub.start_date and sub.end_date
),
with_dims as (
    -- Attach customers attributes
    select
        e.id,
        e.customer_id,
        e.account_type,
        e.balance,
        e.opened_date,
        e.status,
        e.opened_date_plus_21d,
        e.subscription_id,
        e.plan,
        e.monthly_fee,
        p.city
    from attributed e
    left join customers p
        on e.customer_id <=> p.id
    where p.age >= 37
),
eligible as (
    -- Anti-join via a correlated subquery
    select s.*
    from with_dims s
    where not exists (
            select 1
            from transactions ch
            where ch.account_id = s.id
              and ch.txn_time >= s.opened_date
        )
),
daily as (
    -- Multi-level aggregation
    select
        case when monthly_fee >= 263 then 'HIGH' else 'LOW' end as monthly_fee_tier,
        count(*) filter (where plan = 'premium') as premium_rows,
        avg(monthly_fee) as avg_monthly_fee,
        sort_array(collect_set(status)) as status_list
    from eligible
    group by 1
    having count(*) filter (where plan = 'premium') > 1
)
select monthly_fee_tier, avg_monthly_fee
from daily;
