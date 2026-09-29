with with_gaps as (
    select *, case when lag(txn_time) over (partition by txn_type order by txn_time, id) is null
                 or txn_time > lag(txn_time) over (partition by txn_type order by txn_time, id) + interval 60 days then 1 else 0 end as is_new_session
    from transactions
),
sessionized as (
    select
        id,
        txn_type,
        txn_time,
        is_flagged,
        sum(is_new_session) over (partition by txn_type order by txn_time, id rows between unbounded preceding and current row) as island_id
    from with_gaps
)
select
    id,
    txn_type,
    island_id
from sessionized;
