select
    id,
    method,
    amount,
    min(amount) over (partition by method) method_min_amount
from payments
where to_date(paid_at) >= '2023-07-01';
