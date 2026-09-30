select
    id,
    amount,
    method,
    lower(status) as status_lower,
    hour(paid_at) as paid_at_hour
from payments
where paid_at < '2024-06-01' and status = 'FAILED'
order by amount desc
