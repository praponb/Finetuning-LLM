with cleaned as (
    -- Basic cleansing and defaults
    select
        *,
        round(amount, 2) amount_rounded,
        coalesce(method, 'N/A') method_filled
    from payments
    where paid_at > timestamp '2023-11-04 05:00:00'
      and method is distinct from 'cash'
),
gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when paid_at - lag(paid_at) over (partition by status order by paid_at, id) > interval '90' day then 1 else 0 end as session_start_flag
    from cleaned
),
sessions_payments as (
    -- Running sum of the flag = session number
    select
        id,
        method,
        amount,
        paid_at,
        status,
        method_filled,
        sum(session_start_flag) over (partition by status order by paid_at, id rows between unbounded preceding and current row) as session_no
    from gaps
),
flagged as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(paid_at) over (partition by method order by paid_at, id) is null
                 or paid_at > lag(paid_at) over (partition by method order by paid_at, id) + interval '7' day then 1 else 0 end as is_new_session
    from sessions_payments
),
islands as (
    -- Running sum of the flag = session number
    select
        id,
        method,
        paid_at,
        status,
        method_filled,
        session_no,
        sum(is_new_session) over (partition by method order by paid_at, id rows between unbounded preceding and current row) as island_id
    from flagged
),
enriched as (
    -- Running and moving aggregates
    select
        id,
        method,
        paid_at,
        status,
        method_filled,
        session_no,
        island_id,
        lag(island_id, 1) over (partition by method order by island_id, id) prev_island_id,
        percent_rank() over (partition by method, status order by island_id) island_id_percent_rank,
        ntile(5) over (partition by method order by island_id, id) island_id_bucket,
        sum(island_id) over (partition by status order by paid_at, id rows between unbounded preceding and current row) cumulative_island_id,
        avg(session_no) over (partition by status order by paid_at, id rows between 2 preceding and current row) session_no_avg_2
    from islands
)
-- Native SQL PIVOT
select *
from (
    select
        method,
        status,
        island_id_percent_rank
    from enriched
) src
pivot (
    sum(island_id_percent_rank) as value_sum,
    count(1) as cnt
    for status in ('REFUNDED' as refunded, 'FAILED' as failed, 'SUCCESS' as success)
);
