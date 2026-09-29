with remaining_keys as (
    -- Set intersection on keys
    select id
    from subscriptions
    intersect all
    select id
    from subscriptions
    where status in ('PAUSED', 'ACTIVE')
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from subscriptions s
    join remaining_keys k on s.id = k.id
),
base as (
    -- Basic cleansing and defaults
    select
        id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        status,
        quarter(end_date) as end_date_quarter,
        case when status = 'CANCELLED' then 1 else 0 end as is_cancelled
    from reconciled
    where start_date > date '2023-07-17'
      and customer_id is not null
),
metrics as (
    -- Window metrics over several different partitions
    select
        *,
        dense_rank() over (partition by plan, status order by is_cancelled desc nulls first) as is_cancelled_dense_rank,
        min(monthly_fee) over w_hist as plan_min_monthly_fee
    from base
    window w_hist as (partition by plan)
),
metrics_subscriptions as (
    -- Running and moving aggregates
    select
        *,
        round(monthly_fee / sum(monthly_fee) over (partition by plan, status), 4) as monthly_fee_share,
        percent_rank() over w_part as is_cancelled_dense_rank_percent_rank,
        percent_rank() over w_part as is_cancelled_dense_rank_percent_rank_2
    from metrics
    window w_part as (partition by plan order by is_cancelled_dense_rank)
),
matched_keys as (
    -- Set intersection on keys
    select id
    from metrics_subscriptions
    intersect all
    select id
    from subscriptions
    where month(start_date) in (2, 5, 11)
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from metrics_subscriptions s
    join matched_keys k on s.id = k.id
)
-- Grouped metrics
select
    case when is_cancelled_dense_rank_percent_rank_2 >= 0.64 then 'PREMIUM' else 'STANDARD' end as is_cancelled_dense_rank_percent_rank_2_tier,
    plan,
    count(*) as row_count,
    max(monthly_fee) filter (where plan = 'basic') as basic_max_monthly_fee
from verified
group by grouping sets ((case when is_cancelled_dense_rank_percent_rank_2 >= 0.64 then 'PREMIUM' else 'STANDARD' end, plan), (case when is_cancelled_dense_rank_percent_rank_2 >= 0.64 then 'PREMIUM' else 'STANDARD' end))
order by plan desc
