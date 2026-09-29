with enriched as (
    -- Window metrics over several different partitions
    select
        *,
        sum(amount) over (partition by account_id order by txn_time, id rows between unbounded preceding and current row) as running_amount,
        lag(amount, 1) over (partition by merchant order by amount, id) as prev_amount,
        dense_rank() over (partition by account_id order by amount nulls last) as amount_dense_rank,
        rank() over (partition by merchant order by amount nulls last) as amount_rank,
        sum(amount) over (partition by merchant order by amount, id rows between unbounded preceding and current row) as cumulative_amount
    from transactions
),
enriched_transactions as (
    -- Attach accounts attributes
    select
        src.*,
        b.account_type,
        b.status
    from enriched src
    left join accounts b
        on src.account_id = b.id
),
valid as (
    -- Basic cleansing and defaults
    select
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        is_flagged,
        prev_amount,
        amount_dense_rank,
        cumulative_amount,
        account_type,
        status,
        case when merchant in ('Lazada', 'Grab') then 'Tier 1' else 'Other' end as merchant_group,
        coalesce(account_type, 'UNKNOWN') as account_type_clean
    from enriched_transactions
    where txn_time >= date '2023-09-06'
      and merchant is not null
      and txn_type = 'DEBIT'
),
enriched_transactions_2 as (
    -- Ranking and gap features
    select
        *,
        row_number() over (order by amount_dense_rank desc, id) as row_seq,
        last_value(account_type, true) over (partition by status order by amount, id rows between unbounded preceding and current row) as last_known_account_type
    from valid
),
deduped as (
    -- QUALIFY-style window filter
    select *, row_number() over (partition by txn_type order by prev_amount desc, id) as dedup_rank
    from enriched_transactions_2
    qualify dedup_rank = 1
),
with_prev as (
    -- Previous event time per key
    select *, lag(txn_time) over (partition by account_type order by txn_time, id) as prev_txn_time
    from deduped
),
with_gaps as (
    -- Flag rows that start a new session
    select *, case when prev_txn_time is null or txn_time > prev_txn_time + interval '30' day then 1 else 0 end as is_new_session
    from with_prev
),
islands as (
    -- Running sum of the flag = session number
    select *, sum(is_new_session) over (partition by account_type order by txn_time, id rows between unbounded preceding and current row) as session_no
    from with_gaps
)
select
    id,
    account_id,
    txn_type,
    amount,
    txn_time,
    prev_amount,
    account_type,
    status,
    merchant_group
from islands
order by account_id desc, txn_time desc
