select
    id,
    paid_at,
    status,
    order_id
from payments
where order_id is null;
