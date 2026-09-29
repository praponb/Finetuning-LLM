with numbered as (
    select
        id,
        merchant,
        txn_time,
        account_id,
        rank() over (partition by merchant order by txn_time desc) as pos
    from transactions
)
select
    id,
    merchant,
    txn_time,
    account_id
from numbered
where pos <= 3;
