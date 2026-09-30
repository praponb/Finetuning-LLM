with filtered as (
    -- Filter the raw rows and derive helper columns
    select *, datediff(current_date(), start_date) as days_since_start_date
    from subscriptions
    where year(start_date) = 2023
),
matched_keys as (
    -- Set difference on keys
    select plan, customer_id
    from filtered
    except
    select plan, customer_id
    from subscriptions
    where plan != 'standard'
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from filtered s
    join matched_keys k on s.plan = k.plan and s.customer_id = k.customer_id
),
with_dims as (
    -- Attach customers attributes
    select src.*, b.email
    from reconciled src
    join customers b
        on src.customer_id <=> b.id
    where b.signup_date >= '2024-12-01'
),
with_windows as (
    -- Window metrics over several different partitions
    select
        id,
        customer_id,
        plan,
        percent_rank() over (partition by plan order by days_since_start_date nulls first) as days_since_start_date_percent_rank,
        rank() over (order by monthly_fee) as monthly_fee_rank
    from with_dims
),
scored as (
    -- Window metrics over several different partitions
    select
        *,
        round(monthly_fee_rank / sum(monthly_fee_rank) over (partition by customer_id), 4) as monthly_fee_rank_share,
        rank() over (order by days_since_start_date_percent_rank nulls first) as days_since_start_date_percent_rank_rank,
        count(days_since_start_date_percent_rank) over (partition by customer_id, plan) as customer_id_count_days_since_start_date_percent_rank
    from with_windows
)
-- Native SQL UNPIVOT (wide to long)
select
    customer_id,
    kpi,
    metric_value
from (
    select
        customer_id,
        days_since_start_date_percent_rank,
        monthly_fee_rank_share
    from scored
) src
unpivot include nulls (
    metric_value for kpi in (days_since_start_date_percent_rank as DAYS_SINCE_START_DATE_PERCENT_RANK_VAL, monthly_fee_rank_share)
) unp
