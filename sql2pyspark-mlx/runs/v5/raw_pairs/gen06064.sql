with valid as (
    -- Basic cleansing and defaults
    select
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        merchant,
        hour(txn_time) as txn_time_hour,
        hour(txn_time) as txn_time_hour_2
    from transactions
    where txn_time > timestamp '2023-06-06 23:00:00'
      and merchant is not null
),
with_dims as (
    -- Attach accounts attributes
    select
        src.id,
        src.account_id,
        src.txn_type,
        src.amount,
        src.txn_time,
        src.txn_time_hour,
        a.opened_date,
        a.account_type
    from valid src
    inner join accounts a
        on src.account_id = a.id
),
base as (
    -- Keep recent, valid rows
    select
        *,
        lower(txn_type) as txn_type_lower,
        coalesce(account_type, 'N/A') as account_type_filled
    from with_dims
    where account_type is distinct from 'CREDIT'
),
filtered as (
    -- Basic cleansing and defaults
    select
        *,
        case when amount >= 4300 then 'High' when amount >= 1000 then 'Medium' else 'Low' end as amount_band,
        coalesce(account_type, 'UNKNOWN') as account_type_filled_2
    from base
    where txn_type in ('DEBIT', 'REFUND')
),
metrics as (
    -- Running and moving aggregates
    select
        id,
        account_id,
        txn_type,
        amount,
        txn_time_hour,
        opened_date,
        account_type,
        account_type_filled,
        amount_band,
        lead(txn_time_hour) over (partition by txn_type order by amount, id) as next_txn_time_hour,
        lag(txn_time_hour) over (partition by txn_type order by amount, id) as prev_txn_time_hour,
        lead(amount) over (partition by txn_type order by amount, id) as next_amount,
        row_number() over (partition by txn_type order by amount, id) as row_seq
    from filtered
)
select
    id,
    txn_type,
    amount,
    opened_date,
    account_type_filled,
    amount_band,
    prev_txn_time_hour,
    next_amount,
    row_seq
from metrics
order by txn_type desc nulls last, row_seq
