select
    s.id,
    s.city,
    s.region,
    s.sqft,
    latest.status,
    latest.order_shipping_fee
from stores s
left join lateral (
        select c.status, c.shipping_fee as order_shipping_fee
        from orders c
        where c.store_id = s.id
        order by c.amount desc, c.id desc
        limit 1
    ) latest on true;
