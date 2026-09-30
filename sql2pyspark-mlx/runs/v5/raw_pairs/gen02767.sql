select
    id,
    status,
    paid_at,
    method,
    first_value(method) over (partition by status order by paid_at, id) as first_method
from payments;
