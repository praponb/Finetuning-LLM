with valid as (
    -- Filter the raw rows and derive helper columns
    select *, substring(status, 1, 2) as status_prefix
    from subscriptions
    where end_date >= date '2023-08-30'
      and customer_id is not null
),
remaining_keys as (
    -- Set intersection on keys
    select customer_id, plan
    from valid
    intersect
    select customer_id, plan
    from subscriptions
    where start_date between '2024-10-27' and '2025-08-01'
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from valid s
    join remaining_keys k on s.customer_id = k.customer_id and s.plan = k.plan
),
cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        status,
        substring(status, 1, 3) as status_prefix_2
    from verified
    where customer_id is not null
),
filtered as (
    -- Keep recent, valid rows
    select *, datediff(current_date(), end_date) as days_since_end_date
    from cleaned
    where end_date >= to_date('2023-08-07')
),
with_dims as (
    -- Attach customers attributes
    select
        s.id,
        s.customer_id,
        s.start_date,
        s.end_date,
        s.status,
        s.status_prefix_2,
        s.days_since_end_date,
        d.country
    from filtered s
    join customers d
        on s.customer_id = d.id
),
totals as (
    -- Aggregate with conditional metrics
    select
        case when days_since_end_date >= 84 then 'HIGH' else 'LOW' end as days_since_end_date_tier,
        avg(days_since_end_date) as avg_days_since_end_date,
        sum(days_since_end_date) filter (where country = 'US') as us_total_days_since_end_date,
        max(days_since_end_date) as max_days_since_end_date,
        sum(case when country = 'US' then days_since_end_date else 0 end) as us_days_since_end_date
    from with_dims
    group by days_since_end_date_tier
)
select
    days_since_end_date_tier,
    avg_days_since_end_date,
    us_total_days_since_end_date,
    max_days_since_end_date,
    us_days_since_end_date
from totals
order by us_days_since_end_date desc, us_total_days_since_end_date;
