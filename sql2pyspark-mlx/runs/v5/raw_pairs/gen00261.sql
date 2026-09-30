with filtered as (
    -- Basic cleansing and defaults
    select
        id,
        start_date,
        end_date,
        monthly_fee,
        status,
        datediff(start_date, end_date) as days_end_date_to_start_date
    from subscriptions
    where customer_id is not null
      and status <> 'PAUSED'
),
scored as (
    -- Running and moving aggregates
    select
        *,
        row_number() over w_recent as seq_no,
        dense_rank() over (partition by status order by monthly_fee desc) as monthly_fee_dense_rank,
        ntile(10) over w_ordered as days_end_date_to_start_date_bucket,
        row_number() over w_ordered as event_seq,
        lead(days_end_date_to_start_date, 1, 0) over w_ordered as next_days_end_date_to_start_date
    from filtered
    where status in ('ACTIVE', 'PAUSED', 'CANCELLED')
    window w_recent as (partition by status order by days_end_date_to_start_date desc, id), w_ordered as (partition by status order by days_end_date_to_start_date, id)
),
valid as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        start_date,
        end_date,
        status,
        seq_no,
        monthly_fee_dense_rank,
        days_end_date_to_start_date_bucket,
        next_days_end_date_to_start_date,
        case when status in ('CANCELLED', 'ACTIVE') then 'Tier 1' else 'Other' end as status_group
    from scored
    where end_date > to_date('2023-06-17')
      and seq_no <> 6
),
with_windows as (
    -- Ranking and gap features
    select
        *,
        ntile(5) over (partition by status order by seq_no desc, id) as seq_no_bucket,
        dense_rank() over (order by next_days_end_date_to_start_date) as next_days_end_date_to_start_date_dense_rank,
        lead(next_days_end_date_to_start_date, 1) over (partition by status order by start_date, id) as next_next_days_end_date_to_start_date
    from valid
),
metrics as (
    -- Running and moving aggregates
    select
        id,
        status,
        seq_no,
        monthly_fee_dense_rank,
        days_end_date_to_start_date_bucket,
        seq_no_bucket,
        next_days_end_date_to_start_date_dense_rank,
        min(next_days_end_date_to_start_date_dense_rank) over w_running as last7_min_next_days_end_date_to_start_date_dense_rank,
        avg(next_days_end_date_to_start_date_dense_rank) over w_running as last7_avg_next_days_end_date_to_start_date_dense_rank,
        max(next_days_end_date_to_start_date_dense_rank) over w_running as last7_max_next_days_end_date_to_start_date_dense_rank,
        sum(next_days_end_date_to_start_date_dense_rank) over w_running as last7_sum_next_days_end_date_to_start_date_dense_rank
    from with_windows
    window w_running as (partition by status order by next_next_days_end_date_to_start_date, id rows between 6 preceding and current row)
)
-- Native SQL UNPIVOT (wide to long)
select
    id,
    status,
    measure,
    reading
from (
    select
        id,
        status,
        last7_max_next_days_end_date_to_start_date_dense_rank,
        last7_min_next_days_end_date_to_start_date_dense_rank,
        last7_sum_next_days_end_date_to_start_date_dense_rank
    from metrics
) src
unpivot (
    reading for measure in (last7_max_next_days_end_date_to_start_date_dense_rank as LAST7_MAX_NEXT_DAYS_END_DATE_TO_START_DATE_DENSE_RANK, last7_min_next_days_end_date_to_start_date_dense_rank as LAST7_MIN_NEXT_DAYS_END_DATE_TO_START_DATE_DENSE_RANK, last7_sum_next_days_end_date_to_start_date_dense_rank)
) unp;
