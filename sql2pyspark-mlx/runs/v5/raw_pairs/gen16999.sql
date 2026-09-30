select
    oi.product_id,
    o.channel,
    s.store_name,
    s.opened_date
from order_items oi
left outer join orders o on oi.order_id = o.id
full outer join stores s on o.store_id = s.id
