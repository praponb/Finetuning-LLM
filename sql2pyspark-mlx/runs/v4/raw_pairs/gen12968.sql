select
    oi.quantity,
    oi.order_id,
    oi.unit_price,
    o.status
from order_items oi
full outer join orders o on o.id = oi.order_id;
