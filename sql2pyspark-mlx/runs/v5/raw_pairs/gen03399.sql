with cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        customer_id,
        plan,
        end_date,
        status,
        initcap(trim(plan)) as plan_title,
        cast(monthly_fee as int) as monthly_fee_int
    from subscriptions
    where customer_id in (11, 20)
),
remaining_keys as (
    -- Set intersection on keys
    select status, id
    from cleaned
    intersect all
    select status, id
    from subscriptions
    where monthly_fee is not null
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from cleaned s
    inner join remaining_keys k on s.status = k.status and s.id = k.id
),
filtered as (
    -- Basic cleansing and defaults
    select
        *,
        coalesce(monthly_fee_int, 0) as monthly_fee_int_filled,
        upper(status) as status_upper
    from kept_rows
    where customer_id is not null
),
with_dims as (
    -- Attach customers attributes
    select
        src.customer_id,
        src.end_date,
        src.monthly_fee_int,
        src.monthly_fee_int_filled,
        c.city,
        c.name,
        c.credit_limit
    from filtered src
    inner join customers c
        on src.customer_id <=> c.id
),
cleaned_subscriptions as (
    -- Filter the raw rows and derive helper columns
    select
        customer_id,
        end_date,
        monthly_fee_int,
        monthly_fee_int_filled,
        city,
        name,
        credit_limit,
        upper(name) as name_upper,
        round(credit_limit, 0) as credit_limit_rounded
    from with_dims
    where end_date > '2024-07-01'
),
base as (
    -- Filter the raw rows and derive helper columns
    select
        customer_id,
        monthly_fee_int,
        city,
        name,
        credit_limit,
        name_upper,
        credit_limit_rounded,
        monthly_fee_int * monthly_fee_int_filled as monthly_fee_int_times_monthly_fee_int_filled
    from cleaned_subscriptions
    where end_date > to_date('2023-11-08')
      and city is distinct from 'Tokyo'
),
kept_keys as (
    -- Set intersection on keys
    select customer_id
    from base
    intersect
    select customer_id
    from subscriptions
    where plan in ('premium', 'standard')
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from base s
    inner join kept_keys k on s.customer_id = k.customer_id
)
-- Ranking and gap features
select
    customer_id,
    city,
    name,
    name_upper,
    credit_limit_rounded,
    dense_rank() over (order by monthly_fee_int_times_monthly_fee_int_filled nulls first) as monthly_fee_int_times_monthly_fee_int_filled_dense_rank,
    percent_rank() over (partition by city order by monthly_fee_int_times_monthly_fee_int_filled desc nulls first) as monthly_fee_int_times_monthly_fee_int_filled_percent_rank,
    cume_dist() over (partition by customer_id, city order by monthly_fee_int desc nulls first) as monthly_fee_int_cume_dist
from verified;
