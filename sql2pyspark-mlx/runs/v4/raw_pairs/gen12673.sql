with filtered as (
    select *, trim(status) as clean_status
    from accounts
    where opened_date > date '2024-02-13'
      and status is not null
),
with_latest as (
    select
        s.*,
        x.txn_type,
        x.transaction_txn_time
    from filtered s
    left join lateral (
            select c.txn_type, c.txn_time as transaction_txn_time
            from transactions c
            where c.account_id = s.id
            order by c.txn_time desc, c.id desc
            limit 1
        ) x on true
),
scored as (
    select
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        status,
        clean_status,
        txn_type,
        transaction_txn_time,
        lag(balance, 1) over (partition by account_type order by transaction_txn_time asc, id asc) as prev_balance,
        ntile(3) over (order by balance asc, id asc) as balance_bucket,
        dense_rank() over (partition by customer_id order by balance desc) as balance_dense_rank,
        ntile(4) over (partition by status order by balance asc, id asc) as balance_bucket_2
    from with_latest
)
select
    id,
    customer_id,
    balance,
    opened_date,
    status,
    clean_status,
    prev_balance,
    balance_dense_rank,
    balance_bucket_2
from scored
order by balance_dense_rank desc;
