select
    id,
    amount,
    order_id,
    case when amount >= 1640 then 'High' when amount >= 750 then 'Medium' else 'Low' end amount_tier
from payments
where status in ('REFUNDED', 'SUCCESS', 'FAILED');
