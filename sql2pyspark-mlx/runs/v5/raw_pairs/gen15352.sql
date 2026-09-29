select
    id,
    paid_at,
    amount,
    method,
    status,
    lag(amount, 1) over (order by paid_at, id) prev_amount,
    amount - lag(amount, 1) over (partition by method, status order by paid_at, id) amount_change
from payments
