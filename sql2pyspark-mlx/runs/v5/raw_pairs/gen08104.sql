select
    id,
    paid_at,
    amount,
    round(avg(amount) over (order by paid_at, id rows between 2 preceding and current row), 2) as amount_moving_avg_3
from payments
