with enriched as (
    -- Running and moving aggregates
    select
        id,
        plan,
        monthly_fee,
        status,
        lead(monthly_fee) over w_ordered as next_monthly_fee,
        lag(monthly_fee) over w_ordered as prev_monthly_fee,
        row_number() over w_ordered as row_seq
    from subscriptions
    window w_ordered as (partition by plan order by monthly_fee, id)
),
metrics as (
    -- Running and moving aggregates
    select
        id,
        plan,
        status,
        next_monthly_fee,
        row_seq,
        row_number() over (partition by plan, status order by prev_monthly_fee desc, id) as event_seq,
        lead(monthly_fee, 1) over (partition by status order by next_monthly_fee, id) as next_monthly_fee_2,
        min(next_monthly_fee) over (partition by status) as status_min_next_monthly_fee
    from enriched
    where plan not in ('standard', 'enterprise')
),
latest as (
    -- QUALIFY-style window filter
    select *
    from metrics
    qualify row_number() over (partition by status, plan order by row_seq nulls last, id) = 1
),
valid as (
    -- Keep recent, valid rows
    select
        *,
        trim(plan) as clean_plan,
        coalesce(row_seq, 0) as row_seq_filled
    from latest
    where status_min_next_monthly_fee < 225
)
-- Grouped metrics
select
    case when row_seq >= 8 then 'PREMIUM' else 'STANDARD' end as row_seq_tier,
    plan,
    percentile_approx(row_seq, array(0.25, 0.5, 0.75)) as row_seq_quartiles,
    count(*) as num_rows,
    size(collect_set(plan)) as num_plans
from valid
group by case when row_seq >= 8 then 'PREMIUM' else 'STANDARD' end, plan
order by row_seq_tier desc
