select
    id,
    method,
    order_id,
    paid_at,
    amount,
    lead(amount, 1) over (partition by method, order_id order by paid_at, id) as next_amount,
    first_value(amount) over (partition by order_id order by paid_at, id) as first_amount
from payments
