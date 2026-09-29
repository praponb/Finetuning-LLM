with flagged as (
    select *, case when txn_time - lag(txn_time) over (partition by merchant order by txn_time, id) > interval '30' day then 1 else 0 end as session_start_flag
    from transactions
)
select *, sum(session_start_flag) over (partition by merchant order by txn_time, id rows between unbounded preceding and current row) as session_id
from flagged;
