with cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        start_date,
        status,
        ceil(monthly_fee) as monthly_fee_whole
    from subscriptions
    where year(start_date) >= 2025
),
with_windows as (
    -- Running and moving aggregates
    select
        id,
        start_date,
        status,
        count(monthly_fee_whole) over w_grp as running_count_monthly_fee_whole,
        min(monthly_fee_whole) over w_grp as running_min_monthly_fee_whole,
        sum(monthly_fee_whole) over w_grp as running_sum_monthly_fee_whole,
        dense_rank() over (partition by status order by monthly_fee_whole desc nulls first) as monthly_fee_whole_dense_rank
    from cleaned
    where monthly_fee_whole is not null
    window w_grp as (partition by status order by start_date, id rows between unbounded preceding and current row)
),
base as (
    -- Basic cleansing and defaults
    select
        id,
        start_date,
        status,
        running_count_monthly_fee_whole,
        cast(monthly_fee_whole_dense_rank as double) as monthly_fee_whole_dense_rank_num
    from with_windows
    where start_date > date '2023-03-25'
      and start_date between '2023-08-20' and '2025-10-01'
),
scored as (
    -- Ranking and gap features
    select
        id,
        status,
        running_count_monthly_fee_whole,
        monthly_fee_whole_dense_rank_num,
        min(running_count_monthly_fee_whole) over w_recent as running_min_running_count_monthly_fee_whole,
        sum(running_count_monthly_fee_whole) over w_recent as running_sum_running_count_monthly_fee_whole,
        avg(running_count_monthly_fee_whole) over w_recent as running_avg_running_count_monthly_fee_whole,
        dense_rank() over (partition by status order by running_count_monthly_fee_whole nulls last) as running_count_monthly_fee_whole_dense_rank
    from base
    window w_recent as (partition by status order by running_count_monthly_fee_whole, id rows between unbounded preceding and current row)
)
select
    id,
    status,
    running_count_monthly_fee_whole,
    monthly_fee_whole_dense_rank_num,
    running_min_running_count_monthly_fee_whole,
    running_sum_running_count_monthly_fee_whole,
    running_count_monthly_fee_whole_dense_rank,
    case when status rlike '^(CANCELLED|EXPIRED)$' then 'EXTENDED' when status rlike '^(ACTIVE|PAUSED)$' then 'GROUP_A' else 'PRIORITY' end as status_zone
from scored
order by running_count_monthly_fee_whole_dense_rank nulls last;
