with valid as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        account_id,
        amount,
        txn_time,
        merchant,
        is_flagged,
        lower(merchant) as merchant_lower
    from transactions
    where txn_time >= timestamp '2023-10-05 09:00:00'
      and merchant is not null
),
gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(txn_time) over (partition by merchant order by txn_time, id) is null
                 or txn_time > lag(txn_time) over (partition by merchant order by txn_time, id) + interval 30 days then 1 else 0 end as is_new_burst
    from valid
),
sessions_transactions as (
    -- Running sum of the flag = session number
    select *, sum(is_new_burst) over (partition by merchant order by txn_time, id rows between unbounded preceding and current row) as session_no
    from gaps
),
remaining_keys as (
    -- Set intersection on keys
    select account_id, merchant
    from sessions_transactions
    intersect
    select account_id, merchant
    from transactions
    where not is_flagged
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from sessions_transactions s
    inner join remaining_keys k on s.account_id = k.account_id and s.merchant = k.merchant
),
filtered as (
    -- Keep recent, valid rows
    select
        *,
        initcap(trim(merchant)) as merchant_title,
        session_no - is_new_burst as session_no_minus_is_new_burst
    from verified
    where merchant not in ('7-Eleven', 'Shell')
)
select
    id,
    account_id,
    merchant_lower,
    is_new_burst,
    session_no,
    merchant_title,
    session_no_minus_is_new_burst
from filtered
order by account_id nulls last;
