with flagged as (
    select *, case when lag(txn_time) over (partition by account_id order by txn_time, id) is null
                 or txn_time > lag(txn_time) over (partition by account_id order by txn_time, id) + interval '60' day then 1 else 0 end as is_new_burst
    from transactions
)
select
    id,
    account_id,
    txn_time,
    merchant,
    is_flagged,
    sum(is_new_burst) over (partition by account_id order by txn_time, id rows between unbounded preceding and current row) as island_id
from flagged
