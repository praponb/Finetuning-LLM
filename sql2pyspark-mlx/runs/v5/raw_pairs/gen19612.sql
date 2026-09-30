with gaps as (
    select *, case when lag(paid_at) over (partition by status order by paid_at, id) is null
                 or paid_at > lag(paid_at) over (partition by status order by paid_at, id) + interval 60 days then 1 else 0 end as session_start_flag
    from payments
)
select
    id,
    order_id,
    method,
    amount,
    paid_at,
    status,
    sum(session_start_flag) over (partition by status order by paid_at, id rows between unbounded preceding and current row) as session_no
from gaps;
