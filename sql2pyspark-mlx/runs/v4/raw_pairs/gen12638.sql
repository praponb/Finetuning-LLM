with metrics as (
    -- Ranking and gap features
    select
        *,
        sum(amount) over (partition by merchant order by txn_time, id rows between unbounded preceding and current row) running_amount,
        last_value(amount, true) over w_recent last_known_amount
    from transactions
    window w_recent as (partition by account_id, txn_type order by amount, id rows between unbounded preceding and current row)
),
deduped as (
    -- QUALIFY-style window filter
    select *
    from metrics
    qualify row_number() over (partition by merchant order by running_amount, id) = 1
),
flagged as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when txn_time - lag(txn_time) over (partition by account_id order by txn_time, id) > interval '14' day then 1 else 0 end as is_new_burst
    from deduped
),
sessions_transactions as (
    -- Running sum of the flag = session number
    select *, sum(is_new_burst) over (partition by account_id order by txn_time, id rows between unbounded preceding and current row) as burst_id
    from flagged
),
scored as (
    -- Ranking and gap features
    select
        *,
        min(amount) over (partition by merchant) merchant_min_amount,
        row_number() over (partition by txn_type order by is_new_burst, id) row_seq
    from sessions_transactions
),
gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(txn_time) over (partition by account_id order by txn_time, id) is null
                 or txn_time > lag(txn_time) over (partition by account_id order by txn_time, id) + interval 14 days then 1 else 0 end as session_start_flag
    from scored
),
sessions_transactions_2 as (
    -- Running sum of the flag = session number
    select *, sum(session_start_flag) over (partition by account_id order by txn_time, id rows between unbounded preceding and current row) as burst_id_2
    from gaps
)
select
    id,
    account_id,
    txn_type,
    amount,
    txn_time,
    merchant,
    is_flagged,
    running_amount,
    is_new_burst
from sessions_transactions_2
order by merchant
