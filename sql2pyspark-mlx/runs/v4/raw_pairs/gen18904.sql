with with_gaps as (
    select *, case when txn_time - lag(txn_time) over (partition by account_id order by txn_time, id) > interval 60 days then 1 else 0 end as is_new_burst
    from transactions
)
select *, sum(is_new_burst) over (partition by account_id order by txn_time, id rows between unbounded preceding and current row) as island_id
from with_gaps
