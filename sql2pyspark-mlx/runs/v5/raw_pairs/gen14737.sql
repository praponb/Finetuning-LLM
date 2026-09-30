with with_gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when txn_time - lag(txn_time) over (partition by merchant order by txn_time, id) > interval 7 days then 1 else 0 end as new_group_flag
    from transactions
),
islands as (
    -- Running sum of the flag = session number
    select *, sum(new_group_flag) over (partition by merchant order by txn_time, id rows between unbounded preceding and current row) as island_id
    from with_gaps
),
scored as (
    -- Ranking and gap features
    select
        id,
        amount,
        txn_time,
        merchant,
        is_flagged,
        ntile(3) over (partition by account_id order by island_id desc, id) as island_id_bucket,
        lead(new_group_flag, 1) over (order by amount, id) as next_new_group_flag,
        row_number() over (partition by txn_type order by new_group_flag desc, id) as row_seq,
        ntile(5) over (partition by account_id order by island_id, id) as island_id_bucket_2
    from islands
),
deduped as (
    -- QUALIFY-style window filter
    select *, rank() over (partition by merchant order by txn_time) as rn
    from scored
    qualify rn = 1
),
flagged as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(txn_time) over (partition by merchant order by txn_time, id) is null
                 or txn_time > lag(txn_time) over (partition by merchant order by txn_time, id) + interval 30 days then 1 else 0 end as is_new_session
    from deduped
),
sessionized as (
    -- Running sum of the flag = session number
    select *, sum(is_new_session) over (partition by merchant order by txn_time, id rows between unbounded preceding and current row) as session_id
    from flagged
),
flagged_2 as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(txn_time) over (partition by merchant order by txn_time, id) is null
                 or txn_time > lag(txn_time) over (partition by merchant order by txn_time, id) + interval '7' day then 1 else 0 end as is_new_burst
    from sessionized
),
sessionized_transactions as (
    -- Running sum of the flag = session number
    select *, sum(is_new_burst) over (partition by merchant order by txn_time, id rows between unbounded preceding and current row) as burst_id
    from flagged_2
),
cleaned as (
    -- Basic cleansing and defaults
    select *, round(session_id, 0) as session_id_rounded
    from sessionized_transactions
    where merchant is not null
),
latest as (
    -- QUALIFY-style window filter
    select *
    from cleaned
    qualify row_number() over (partition by merchant order by session_id_rounded desc nulls first, id) = 1
)
-- Ranking and gap features
select
    *,
    avg(next_new_group_flag) over (partition by merchant) as merchant_avg_next_new_group_flag,
    dense_rank() over w_hist as is_new_burst_dense_rank
from latest
window w_hist as (partition by merchant order by is_new_burst nulls first);
