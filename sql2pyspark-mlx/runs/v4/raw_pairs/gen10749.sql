with flagged as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(txn_time) over (partition by txn_type order by txn_time, id) is null
                 or txn_time > lag(txn_time) over (partition by txn_type order by txn_time, id) + interval 60 days then 1 else 0 end as is_new_session
    from transactions
)
-- Running sum of the flag = session number
select *, sum(is_new_session) over (partition by txn_type order by txn_time, id rows between unbounded preceding and current row) as burst_id
from flagged
