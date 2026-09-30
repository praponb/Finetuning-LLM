select
    id,
    order_id,
    method,
    amount,
    paid_at,
    status,
    percent_rank() over (order by amount desc) as amount_percent_rank,
    ntile(3) over (order by amount desc, id asc) as amount_bucket,
    first_value(amount) ignore nulls over (partition by order_id order by amount asc, id asc rows between unbounded preceding and current row) as first_known_amount
from payments
