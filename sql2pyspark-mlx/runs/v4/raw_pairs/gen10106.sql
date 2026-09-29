with cleaned as (
    -- Filter the raw rows and derive helper columns
    select *, cast(is_flagged as int) as is_flagged_flag
    from transactions
    where txn_time >= date '2024-02-23'
      and merchant is not null
),
ranked as (
    -- Keep one row per key (latest first)
    select *
    from cleaned
    qualify row_number() over (partition by merchant order by amount desc nulls last, id) = 1
),
prev_events as (
    -- Previous event time per key
    select *, lag(txn_time) over (partition by txn_type order by txn_time, id) as prev_txn_time
    from ranked
),
flagged as (
    -- Flag rows that start a new session
    select *, case when prev_txn_time is null or txn_time > prev_txn_time + interval '30' day then 1 else 0 end as is_new_burst
    from prev_events
),
sessions_transactions as (
    -- Running sum of the flag = session number
    select *, sum(is_new_burst) over (partition by txn_type order by txn_time, id rows between unbounded preceding and current row) as burst_id
    from flagged
),
with_prev as (
    -- Previous event time per key
    select *, lag(txn_time) over (partition by txn_type order by txn_time, id) as prev_txn_time_2
    from sessions_transactions
),
gaps as (
    -- Flag rows that start a new session
    select *, case when prev_txn_time_2 is null or txn_time > prev_txn_time_2 + interval 14 days then 1 else 0 end as is_new_burst_2
    from with_prev
),
sessionized as (
    -- Running sum of the flag = session number
    select *, sum(is_new_burst_2) over (partition by txn_type order by txn_time, id rows between unbounded preceding and current row) as island_id
    from gaps
),
scored as (
    -- Ranking and gap features
    select
        *,
        first_value(amount, true) over (partition by merchant order by is_flagged_flag, id rows between unbounded preceding and current row) as first_known_amount,
        row_number() over (order by prev_txn_time, id) as seq_no,
        lead(is_new_burst_2, 1) over (partition by merchant, txn_type order by txn_time, id) as next_is_new_burst_2,
        count(is_new_burst_2) over (partition by merchant order by cast(prev_txn_time as bigint) range between 7776000 preceding and current row) as is_new_burst_2_count_90d,
        lag(burst_id, 1) over w_ordered as prev_burst_id
    from sessionized
    window w_ordered as (partition by account_id, merchant order by prev_txn_time_2, id)
),
scored_transactions as (
    -- Running and moving aggregates
    select
        *,
        avg(amount) over (partition by merchant order by cast(prev_txn_time_2 as bigint) range between 2592000 preceding and current row) as amount_avg_30d,
        first_value(amount) ignore nulls over w_hist as first_known_amount_2,
        min(is_new_burst_2_count_90d) over (partition by account_id) as account_id_min_is_new_burst_2_count_90d,
        row_number() over w_recent as row_seq
    from scored
    window w_hist as (partition by account_id, merchant order by is_new_burst_2_count_90d, id rows between unbounded preceding and current row), w_recent as (order by is_flagged_flag desc, id)
)
-- Multi-level aggregation
select
    island_id,
    date_trunc('year', prev_txn_time_2) as year_start,
    sum(case when merchant = 'Amazon' then is_new_burst else 0 end) as amazon_is_new_burst,
    count(distinct id) as distinct_ids,
    count(*) filter (where merchant = 'Amazon') as amazon_rows,
    rank() over (partition by island_id order by count(distinct id) desc) as distinct_ids_rank
from scored_transactions
group by 1, 2;
