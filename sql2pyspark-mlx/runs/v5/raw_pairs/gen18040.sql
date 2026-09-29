with with_prev as (
    select *, lag(txn_time) over (partition by txn_type order by txn_time asc, id asc) as prev_txn_time
    from transactions
),
flagged as (
    select *, case when prev_txn_time is null or txn_time > prev_txn_time + interval 90 days then 1 else 0 end as session_start_flag
    from with_prev
),
sessions_transactions as (
    select *, sum(session_start_flag) over (partition by txn_type order by txn_time asc, id asc rows between unbounded preceding and current row) as session_id
    from flagged
)
select
    id,
    account_id,
    txn_type,
    amount,
    txn_time,
    merchant,
    is_flagged,
    session_start_flag,
    session_id
from sessions_transactions
order by txn_type asc
