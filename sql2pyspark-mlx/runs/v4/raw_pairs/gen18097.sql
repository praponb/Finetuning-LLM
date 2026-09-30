select
    id,
    paid_at,
    status,
    method,
    concat_ws(',', status, method) as status_method_key,
    round(amount, 0) as amount_rounded
from payments
where status <> 'REFUNDED' and (order_id in (8, 10, 18) or amount <= 1370);
