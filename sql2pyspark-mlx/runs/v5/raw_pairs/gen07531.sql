with base as (
    -- Basic cleansing and defaults
    select *, length(status) as status_length
    from payments
    where paid_at >= timestamp '2023-06-06 21:00:00'
      and method is not null
),
flagged as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(paid_at) over (partition by status order by paid_at, id) is null
                 or paid_at > lag(paid_at) over (partition by status order by paid_at, id) + interval 60 days then 1 else 0 end as session_start_flag
    from base
),
sessions_payments as (
    -- Running sum of the flag = session number
    select *, sum(session_start_flag) over (partition by status order by paid_at, id rows between unbounded preceding and current row) as island_id
    from flagged
),
enriched as (
    -- Ranking and gap features
    select
        id,
        method,
        paid_at,
        session_start_flag,
        island_id,
        count(island_id) over (partition by status order by cast(paid_at as bigint) range between 7776000 preceding and current row) as island_id_count_90d,
        lead(amount, 1, 0) over (partition by method order by island_id, id) as next_amount,
        row_number() over (partition by method order by amount desc, id) as seq_no,
        avg(island_id) over (order by session_start_flag, id rows between 6 preceding and current row) as island_id_avg_6
    from sessions_payments
    where island_id > 7
),
valid as (
    -- Basic cleansing and defaults
    select
        id,
        method,
        paid_at,
        island_id_count_90d,
        next_amount,
        seq_no,
        island_id_avg_6,
        island_id * session_start_flag as island_id_times_session_start_flag
    from enriched
    where to_date(paid_at) < '2025-04-01'
)
select
    id,
    paid_at,
    next_amount,
    island_id_times_session_start_flag
from valid
order by next_amount
