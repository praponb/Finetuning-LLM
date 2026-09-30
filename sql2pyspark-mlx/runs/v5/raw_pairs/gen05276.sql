with with_gaps as (
    select *, case when lag(paid_at) over (partition by status order by paid_at, id) is null
                 or paid_at > lag(paid_at) over (partition by status order by paid_at, id) + interval 60 days then 1 else 0 end as new_group_flag
    from payments
),
sessions_payments as (
    select *, sum(new_group_flag) over (partition by status order by paid_at, id rows between unbounded preceding and current row) as session_no
    from with_gaps
)
select
    id,
    method,
    amount,
    status,
    new_group_flag,
    session_no
from sessions_payments
order by method desc nulls first;
