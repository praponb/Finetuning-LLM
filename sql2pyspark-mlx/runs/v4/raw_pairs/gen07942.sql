select
    oi.id,
    oi.quantity,
    o.channel,
    o.status,
    s.sqft,
    s.store_name
from order_items oi
join orders o on oi.order_id = o.id
join stores s on o.store_id = s.id
where oi.order_id in (31, 34, 40) and s.sqft < 12392
order by s.store_name desc;
