with cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        status,
        round(monthly_fee * 100, 2) as adjusted_monthly_fee
    from subscriptions
    where plan = 'enterprise'
),
ranked as (
    -- QUALIFY-style window filter
    select
        id,
        plan,
        start_date,
        monthly_fee,
        adjusted_monthly_fee
    from (
        select *, rank() over (partition by status order by end_date desc) as rn
        from cleaned
    ) x
    where rn = 1
),
cleaned_subscriptions as (
    -- Keep recent, valid rows
    select
        id,
        start_date,
        monthly_fee,
        adjusted_monthly_fee,
        case when plan = 'premium' then 1 else 0 end as is_premium
    from ranked
    where start_date > date '2023-03-05'
),
metrics as (
    -- Window metrics over several different partitions
    select
        id,
        monthly_fee,
        adjusted_monthly_fee,
        is_premium,
        row_number() over (order by is_premium, id) as seq_no,
        last_value(adjusted_monthly_fee) ignore nulls over (order by adjusted_monthly_fee, id rows between unbounded preceding and current row) as last_known_adjusted_monthly_fee,
        ntile(5) over (order by is_premium, id) as is_premium_bucket
    from cleaned_subscriptions
)
select
    id,
    monthly_fee,
    adjusted_monthly_fee,
    is_premium,
    last_known_adjusted_monthly_fee,
    is_premium_bucket
from metrics
order by adjusted_monthly_fee;
