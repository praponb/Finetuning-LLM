with with_gaps as (
    select *, case when lag(txn_time) over (partition by account_id order by txn_time, id) is null
                 or txn_time > lag(txn_time) over (partition by account_id order by txn_time, id) + interval 14 days then 1 else 0 end as is_new_session
    from transactions
),
sessionized as (
    select
        id,
        account_id,
        amount,
        txn_time,
        merchant,
        is_flagged,
        sum(is_new_session) over (partition by account_id order by txn_time, id rows between unbounded preceding and current row) as session_no
    from with_gaps
)
select
    id,
    merchant,
    is_flagged,
    session_no
from sessionized
order by id desc nulls last;
