select
    oi.product_id,
    oi.unit_price,
    p.category,
    o.discount
from order_items oi
full outer join products p on oi.product_id = p.id
left outer join orders o on oi.order_id = o.id
order by oi.unit_price desc;
