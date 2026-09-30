with flagged as (
    select *, case when lag(paid_at) over (partition by order_id order by paid_at asc, id asc) is null
                 or paid_at > lag(paid_at) over (partition by order_id order by paid_at asc, id asc) + interval '7' day then 1 else 0 end as is_new_burst
    from payments
),
sessions_payments as (
    select *, sum(is_new_burst) over (partition by order_id order by paid_at asc, id asc rows between unbounded preceding and current row) as session_id
    from flagged
)
select
    id,
    amount,
    status
from sessions_payments
order by amount asc nulls first
