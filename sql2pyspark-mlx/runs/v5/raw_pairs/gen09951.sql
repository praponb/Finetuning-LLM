select
    id,
    store_id,
    order_date,
    discount,
    lead(discount, 1) over (partition by store_id order by order_date, id) as next_discount
from orders
order by store_id desc;
