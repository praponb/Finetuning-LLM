with valid as (
    -- Keep recent, valid rows
    select
        id,
        account_id,
        txn_type,
        txn_time,
        merchant,
        is_flagged,
        concat(merchant, ' ', txn_type) as merchant_txn_type
    from transactions
    where txn_time >= timestamp '2023-07-03 16:00:00'
),
with_dims as (
    -- Attach accounts attributes
    select
        e.*,
        d.balance,
        d.status,
        d.opened_date
    from valid e
    left join accounts d
        on e.account_id = d.id
),
with_dims_transactions as (
    -- Attach accounts attributes
    select
        s.*,
        b.account_type,
        b.balance as account_balance
    from with_dims s
    inner join accounts b
        on s.account_id = b.id
),
with_dims_transactions_2 as (
    -- Attach accounts attributes
    select a.*, d.balance as account_balance_2
    from with_dims_transactions a
    left join accounts d
        on a.account_id = d.id
),
gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when txn_time - lag(txn_time) over (partition by status order by txn_time, id) > interval 60 days then 1 else 0 end as new_group_flag
    from with_dims_transactions_2
),
sessionized as (
    -- Running sum of the flag = session number
    select *, sum(new_group_flag) over (partition by status order by txn_time, id rows between unbounded preceding and current row) as session_no
    from gaps
)
select
    s.id,
    s.account_id,
    s.txn_type,
    s.txn_time,
    s.merchant,
    s.is_flagged,
    s.merchant_txn_type,
    s.balance,
    s.status,
    (select sum(t2.amount) from transactions t2 where t2.account_id = s.account_id) as sum_transaction_amount
from sessionized s
order by txn_time nulls last;
