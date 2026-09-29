with attributed as (
    -- Non-equi join on a validity window
    select
        src.*,
        sub.id as subscription_id,
        sub.plan,
        sub.monthly_fee
    from accounts src
    left join subscriptions sub
        on src.customer_id = sub.customer_id
       and src.opened_date >= sub.start_date
       and src.opened_date < sub.end_date + interval '14' day
),
matched_keys as (
    -- Set intersection on keys
    select account_type, status
    from attributed
    intersect
    select account_type, status
    from accounts
    where account_type in ('CHECKING', 'CREDIT', 'SAVINGS')
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from attributed s
    inner join matched_keys k on s.account_type = k.account_type and s.status = k.status
),
kept as (
    -- Anti-join via a correlated subquery
    select s.*
    from verified s
    where not exists (
            select 1
            from transactions ch
            where ch.account_id = s.id
              and ch.txn_time >= s.opened_date
        )
),
enriched as (
    -- Attach customers attributes
    select
        e.id,
        e.customer_id,
        e.balance,
        e.opened_date,
        e.plan,
        e.monthly_fee,
        p.segment,
        p.name
    from kept e
    inner join customers p
        on e.customer_id = p.id
),
base as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        customer_id,
        balance,
        monthly_fee,
        name,
        greatest(monthly_fee, balance) greatest_monthly_fee_balance,
        monthly_fee - balance monthly_fee_minus_balance,
        coalesce(plan, 'UNKNOWN') resolved_plan
    from enriched
    where opened_date > to_date('2023-10-03')
      and plan is distinct from 'premium'
),
metrics as (
    -- Window metrics over several different partitions
    select
        *,
        percent_rank() over (partition by customer_id order by balance) balance_percent_rank,
        max(greatest_monthly_fee_balance) over w_grp customer_id_max_greatest_monthly_fee_balance
    from base
    window w_grp as (partition by customer_id)
),
not_flagged as (
    -- Semi-join via a correlated subquery
    select s.*
    from metrics s
    where exists (
            select 1
            from transactions ch
            where ch.account_id = s.id
              and ch.account_id in (18, 28)
        )
)
-- Join a dimension and aggregate
select
    c.city,
    count(*) as cnt,
    round(sum(e.customer_id_max_greatest_monthly_fee_balance), 2) as total_customer_id_max_greatest_monthly_fee_balance,
    min(e.monthly_fee) as min_monthly_fee,
    grouping_id() as grouping_level
from not_flagged e
inner join customers c on e.customer_id = c.id
group by rollup (c.city)
order by total_customer_id_max_greatest_monthly_fee_balance
