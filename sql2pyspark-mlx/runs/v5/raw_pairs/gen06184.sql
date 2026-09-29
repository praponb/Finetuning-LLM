with with_window as (
    -- Non-equi join on a validity window
    select
        e.*,
        sub.id as subscription_id,
        sub.plan,
        sub.monthly_fee
    from accounts e
    left join subscriptions sub
        on e.customer_id = sub.customer_id
       and e.opened_date >= sub.start_date
       and e.opened_date < sub.end_date + interval 30 days
),
attributed as (
    -- Non-equi join on a validity window
    select
        a.*,
        sub.id as subscription_id_2,
        sub.plan as subscription_plan,
        sub.monthly_fee as subscription_monthly_fee
    from with_window a
    left join subscriptions sub
        on a.customer_id = sub.customer_id
       and a.opened_date between sub.start_date and sub.end_date
),
kept as (
    -- Anti-join via a correlated subquery
    select s.*
    from attributed s
    where not exists (
            select 1
            from transactions x
            where x.account_id = s.id
              and x.txn_time < s.opened_date
        )
),
eligible as (
    -- Anti-join via a correlated subquery
    select s.*
    from kept s
    where not exists (
            select 1
            from transactions ch
            where ch.account_id = s.id
              and ch.txn_time <= s.opened_date
        )
),
ranked as (
    -- QUALIFY-style window filter
    select
        id,
        customer_id,
        subscription_id,
        plan,
        monthly_fee,
        subscription_id_2,
        subscription_plan,
        subscription_monthly_fee
    from (
        select *, row_number() over (partition by plan order by subscription_monthly_fee, id, subscription_id, subscription_id_2) as rn
        from eligible
    ) t
    where rn = 1
)
-- Window metrics over several different partitions
select
    id,
    subscription_id,
    monthly_fee,
    subscription_id_2,
    subscription_plan,
    lead(subscription_monthly_fee) over (partition by customer_id order by subscription_id_2, id, subscription_id, subscription_id_2) as next_subscription_monthly_fee,
    lag(subscription_monthly_fee) over (partition by customer_id order by subscription_id_2, id, subscription_id, subscription_id_2) as prev_subscription_monthly_fee,
    lag(subscription_id) over (partition by customer_id order by subscription_id_2, id, subscription_id, subscription_id_2) as prev_subscription_id,
    lead(subscription_id) over (partition by customer_id order by subscription_id_2, id, subscription_id, subscription_id_2) as next_subscription_id,
    row_number() over (partition by customer_id order by subscription_id_2, id, subscription_id, subscription_id_2) as event_seq,
    last_value(subscription_monthly_fee) ignore nulls over (partition by subscription_plan order by subscription_id, id, subscription_id, subscription_id_2 rows between unbounded preceding and current row) as last_known_subscription_monthly_fee
from ranked
where subscription_id between 4 and 8;
