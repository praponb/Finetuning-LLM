with with_windows as (
    -- Running and moving aggregates
    select
        id,
        method,
        status,
        last_value(method) ignore nulls over (partition by status, method order by paid_at asc, id asc rows between unbounded preceding and current row) as last_known_method,
        lag(amount, 1, 0) over (partition by order_id order by amount asc, id asc) as prev_amount,
        avg(amount) over (partition by method) as method_avg_amount
    from payments
)
select
    id,
    method,
    status,
    last_known_method,
    method_avg_amount
from with_windows
order by status desc nulls last;
