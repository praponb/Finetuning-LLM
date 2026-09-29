select
    s.id,
    s.store_id,
    s.status,
    s.discount,
    s.shipping_fee,
    x.cost
from orders s
left join lateral (
        select c.cost
        from shipments c
        where c.order_id = s.id
        order by c.delivered_date desc, c.id desc
        limit 1
    ) x on true;
