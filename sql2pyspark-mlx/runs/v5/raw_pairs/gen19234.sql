with base as (
    -- Keep recent, valid rows
    select
        *,
        coalesce(amount, 0) as amount_filled,
        coalesce(merchant, 'NONE') as resolved_merchant
    from transactions
    where merchant in ('Starbucks', 'Lazada', '7-Eleven')
),
scored as (
    -- Ranking and gap features
    select
        id,
        amount,
        txn_time,
        merchant,
        is_flagged,
        resolved_merchant,
        sum(amount_filled) over (partition by merchant order by amount_filled, id rows between unbounded preceding and current row) as cumulative_amount_filled,
        dense_rank() over (partition by account_id order by amount_filled nulls last) as amount_filled_dense_rank
    from base
),
with_prev as (
    -- Previous event time per key
    select *, lag(txn_time) over (partition by merchant order by txn_time, id) as prev_txn_time
    from scored
),
gaps as (
    -- Flag rows that start a new session
    select *, case when prev_txn_time is null or txn_time > prev_txn_time + interval 14 days then 1 else 0 end as new_group_flag
    from with_prev
),
sessionized as (
    -- Running sum of the flag = session number
    select *, sum(new_group_flag) over (partition by merchant order by txn_time, id rows between unbounded preceding and current row) as session_id
    from gaps
),
valid as (
    -- Keep recent, valid rows
    select
        id,
        amount,
        txn_time,
        merchant,
        is_flagged,
        resolved_merchant,
        cumulative_amount_filled,
        amount_filled_dense_rank,
        prev_txn_time,
        new_group_flag,
        session_id,
        cumulative_amount_filled + amount as cumulative_amount_filled_plus_amount,
        amount_filled_dense_rank + new_group_flag as amount_filled_dense_rank_plus_new_group_flag
    from sessionized
    where txn_time < '2024-08-01'
)
-- Aggregate with conditional metrics
select
    case when session_id >= 6 then 'PREMIUM' else 'STANDARD' end as session_id_tier,
    case when cumulative_amount_filled_plus_amount >= 68 then 'LARGE' else 'SMALL' end as cumulative_amount_filled_plus_amount_tier,
    case when amount >= 4100 then 'PREMIUM' else 'STANDARD' end as amount_tier,
    max(amount_filled_dense_rank_plus_new_group_flag) as max_amount_filled_dense_rank_plus_new_group_flag,
    round(avg(amount_filled_dense_rank), 2) as avg_amount_filled_dense_rank,
    count(*) filter (where merchant = 'Lazada') as lazada_rows
from valid
group by 1, 2, 3;
