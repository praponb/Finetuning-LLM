with with_gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(txn_time) over (partition by txn_type order by txn_time, id) is null
                 or txn_time > lag(txn_time) over (partition by txn_type order by txn_time, id) + interval '14' day then 1 else 0 end as is_new_session
    from transactions
),
sessionized as (
    -- Running sum of the flag = session number
    select *, sum(is_new_session) over (partition by txn_type order by txn_time, id rows between unbounded preceding and current row) as island_id
    from with_gaps
)
select
    id,
    account_id,
    txn_type,
    txn_time,
    is_flagged,
    is_new_session,
    island_id
from sessionized
order by txn_time desc, is_new_session;
