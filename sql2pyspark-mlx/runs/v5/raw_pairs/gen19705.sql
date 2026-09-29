with latest as (
    -- QUALIFY-style window filter
    select *, row_number() over (partition by status, plan order by end_date desc, id) as row_num
    from subscriptions
    qualify row_num = 1
),
metrics as (
    -- Running and moving aggregates
    select
        id,
        plan,
        status,
        last_value(monthly_fee, true) over (partition by customer_id order by start_date, id rows between unbounded preceding and current row) last_known_monthly_fee,
        percent_rank() over (partition by customer_id, status order by monthly_fee desc nulls first) monthly_fee_percent_rank,
        sum(monthly_fee) over (order by end_date, id rows between unbounded preceding and current row) cumulative_monthly_fee,
        dense_rank() over (partition by plan, customer_id order by monthly_fee nulls first) monthly_fee_dense_rank,
        avg(monthly_fee) over (partition by customer_id order by monthly_fee, id rows between 3 preceding and current row) monthly_fee_avg_3
    from latest
),
with_windows as (
    -- Running and moving aggregates
    select
        *,
        min(monthly_fee_dense_rank) over (partition by plan order by monthly_fee_dense_rank, id rows between unbounded preceding and current row) running_min_monthly_fee_dense_rank,
        avg(monthly_fee_dense_rank) over (partition by plan order by monthly_fee_dense_rank, id rows between unbounded preceding and current row) running_avg_monthly_fee_dense_rank,
        count(monthly_fee_dense_rank) over (partition by plan order by monthly_fee_dense_rank, id rows between unbounded preceding and current row) running_count_monthly_fee_dense_rank,
        max(monthly_fee_dense_rank) over (partition by plan order by monthly_fee_dense_rank, id rows between unbounded preceding and current row) running_max_monthly_fee_dense_rank,
        ntile(3) over (partition by status order by monthly_fee_avg_3, id) monthly_fee_avg_3_bucket
    from metrics
    where status in ('ACTIVE', 'EXPIRED', 'CANCELLED')
),
remaining_keys as (
    -- Set difference on keys
    select status
    from with_windows
    except all
    select status
    from subscriptions
    where plan not in ('enterprise', 'basic')
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from with_windows s
    inner join remaining_keys k on s.status = k.status
),
cleaned as (
    -- Keep recent, valid rows
    select
        id,
        plan,
        status,
        last_known_monthly_fee,
        monthly_fee_percent_rank,
        monthly_fee_dense_rank,
        running_max_monthly_fee_dense_rank,
        monthly_fee_avg_3_bucket,
        coalesce(running_avg_monthly_fee_dense_rank, 0) running_avg_monthly_fee_dense_rank_filled,
        cast(monthly_fee_avg_3_bucket as string) monthly_fee_avg_3_bucket_num
    from verified
    where cumulative_monthly_fee != 853
),
scored as (
    -- Window metrics over several different partitions
    select *, avg(running_avg_monthly_fee_dense_rank_filled) over (partition by status) status_avg_running_avg_monthly_fee_dense_rank_filled
    from cleaned
)
-- Native SQL UNPIVOT (wide to long)
select
    plan,
    kpi,
    val
from (
    select
        plan,
        last_known_monthly_fee,
        monthly_fee_percent_rank,
        running_avg_monthly_fee_dense_rank_filled,
        running_max_monthly_fee_dense_rank
    from scored
) src
unpivot include nulls (
    val for kpi in (last_known_monthly_fee as LAST_KNOWN_MONTHLY_FEE, monthly_fee_percent_rank as MONTHLY_FEE_PERCENT_RANK_VAL, running_avg_monthly_fee_dense_rank_filled, running_max_monthly_fee_dense_rank as RUNNING_MAX_MONTHLY_FEE_DENSE_RANK)
)
