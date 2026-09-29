with flagged as (
    select *, case when txn_time - lag(txn_time) over (partition by merchant order by txn_time, id) > interval '90' day then 1 else 0 end as new_group_flag
    from transactions
)
select *, sum(new_group_flag) over (partition by merchant order by txn_time, id rows between unbounded preceding and current row) as island_id
from flagged
