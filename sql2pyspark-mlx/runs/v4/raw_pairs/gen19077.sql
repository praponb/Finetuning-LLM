select
    o.id,
    o.order_date,
    o.discount,
    o.store_id
from orders o
where not exists (select 1 from payments p where p.order_id = o.id and p.order_id = 29);
