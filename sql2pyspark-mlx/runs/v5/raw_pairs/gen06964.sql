with valid as (
    -- Filter the raw rows and derive helper columns
    select *, substring(plan, 1, 3) as plan_prefix
    from subscriptions
    where end_date >= to_date('2023-08-30')
),
enriched as (
    -- Window metrics over several different partitions
    select
        id,
        customer_id,
        plan,
        end_date,
        monthly_fee,
        count(monthly_fee) over (partition by plan order by start_date, id rows between unbounded preceding and current row) as running_count_monthly_fee,
        max(monthly_fee) over (partition by plan order by start_date, id rows between unbounded preceding and current row) as running_max_monthly_fee,
        sum(monthly_fee) over (partition by plan order by start_date, id rows between unbounded preceding and current row) as running_sum_monthly_fee,
        min(monthly_fee) over (partition by plan order by start_date, id rows between unbounded preceding and current row) as running_min_monthly_fee,
        rank() over (partition by plan order by monthly_fee nulls last) as monthly_fee_rank
    from valid
),
enriched_subscriptions as (
    -- Attach customers attributes
    select
        e.id,
        e.customer_id,
        e.end_date,
        e.running_count_monthly_fee,
        e.running_sum_monthly_fee,
        b.email,
        b.country,
        b.segment
    from enriched e
    left join customers b
        on e.customer_id = b.id
),
base as (
    -- Keep recent, valid rows
    select
        id,
        end_date,
        running_count_monthly_fee,
        running_sum_monthly_fee,
        email,
        country,
        split(lower(email), '@')[0] as email_user,
        coalesce(segment, 'DIRECT') as segment_clean
    from enriched_subscriptions
    where country is not null
)
select
    id,
    end_date,
    running_count_monthly_fee,
    running_sum_monthly_fee,
    country,
    email_user,
    segment_clean
from base
