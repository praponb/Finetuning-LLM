with latest as (
    -- Deduplicate with ROW_NUMBER
    select
        id,
        account_id,
        amount,
        txn_time,
        merchant
    from (
        select *, row_number() over (partition by txn_type order by txn_time desc nulls first, id asc) as row_num
        from transactions
    ) ranked
    where row_num = 1
),
with_gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(txn_time) over (partition by account_id order by txn_time asc, id asc) is null
                 or txn_time > lag(txn_time) over (partition by account_id order by txn_time asc, id asc) + interval 60 days then 1 else 0 end as new_group_flag
    from latest
),
sessions_transactions as (
    -- Running sum of the flag = session number
    select *, sum(new_group_flag) over (partition by account_id order by txn_time asc, id asc rows between unbounded preceding and current row) as burst_id
    from with_gaps
),
with_windows as (
    -- Ranking and gap features
    select
        *,
        lead(new_group_flag) over (partition by account_id, merchant order by burst_id asc, id asc) as next_new_group_flag,
        lag(new_group_flag) over (partition by account_id, merchant order by burst_id asc, id asc) as prev_new_group_flag,
        lead(amount) over (partition by account_id, merchant order by burst_id asc, id asc) as next_amount,
        row_number() over (partition by account_id, merchant order by burst_id asc, id asc) as event_seq
    from sessions_transactions
),
latest_transactions as (
    -- Deduplicate with ROW_NUMBER
    select *
    from with_windows
    qualify row_number() over (partition by account_id order by txn_time asc nulls last, id asc) = 1
),
prev_events as (
    -- Previous event time per key
    select *, lag(txn_time) over (partition by merchant order by txn_time asc, id asc) as prev_txn_time
    from latest_transactions
),
gaps as (
    -- Flag rows that start a new session
    select *, case when prev_txn_time is null or txn_time > prev_txn_time + interval 90 days then 1 else 0 end as new_group_flag_2
    from prev_events
),
sessions_transactions_2 as (
    -- Running sum of the flag = session number
    select *, sum(new_group_flag_2) over (partition by merchant order by txn_time asc, id asc rows between unbounded preceding and current row) as session_no
    from gaps
)
select
    s.id,
    s.account_id,
    s.amount,
    s.merchant,
    s.new_group_flag,
    s.burst_id,
    s.next_new_group_flag,
    s.next_amount,
    s.event_seq,
    case when merchant rlike '^(Grab|Amazon)$' then 'OTHER_ZONE' when merchant rlike '^(7-Eleven|Lazada)$' then 'PRIORITY' else 'EXTENDED' end as merchant_zone,
    (select max(t2.amount) from transactions t2 where t2.account_id = s.account_id) as max_transaction_amount
from sessions_transactions_2 s
order by next_new_group_flag asc nulls last;
