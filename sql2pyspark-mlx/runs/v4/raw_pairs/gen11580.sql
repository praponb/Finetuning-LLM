with flagged as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when txn_time - lag(txn_time) over (partition by account_id order by txn_time, id) > interval '7' day then 1 else 0 end as new_group_flag
    from transactions
)
-- Running sum of the flag = session number
select *, sum(new_group_flag) over (partition by account_id order by txn_time, id rows between unbounded preceding and current row) as island_id
from flagged
