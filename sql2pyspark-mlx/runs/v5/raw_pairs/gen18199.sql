with cleaned as (
    -- Keep recent, valid rows
    select
        *,
        dayofweek(start_date) as start_date_dayofweek,
        length(plan) as plan_length
    from subscriptions
    where end_date >= date '2023-07-01'
      and customer_id is not null
),
deduped as (
    -- Deduplicate with ROW_NUMBER
    select *, row_number() over (partition by status, plan order by start_date nulls first, id) as rn
    from cleaned
    qualify rn = 1
),
enriched as (
    -- Ranking and gap features
    select
        *,
        avg(plan_length) over w_running as running_avg_plan_length,
        min(plan_length) over w_running as running_min_plan_length,
        sum(plan_length) over w_running as running_sum_plan_length
    from deduped
    window w_running as (partition by customer_id order by start_date, id rows between unbounded preceding and current row)
),
with_dims as (
    -- Attach customers attributes
    select
        a.id,
        a.customer_id,
        a.monthly_fee,
        a.status,
        a.start_date_dayofweek,
        a.plan_length,
        a.running_min_plan_length,
        c.city,
        c.country
    from enriched a
    left join customers c
        on a.customer_id = c.id
)
select
    s.id,
    s.customer_id,
    s.plan_length,
    s.running_min_plan_length,
    s.city,
    s.country,
    (select sum(x.balance) from accounts x where x.customer_id = s.customer_id) as sum_account_balance
from with_dims s
order by customer_id nulls last
