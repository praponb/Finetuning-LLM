select
    id,
    paid_at,
    amount,
    method,
    lower(method) as method_lower
from payments
where to_date(paid_at) < '2025-04-01'
order by amount asc, paid_at asc
