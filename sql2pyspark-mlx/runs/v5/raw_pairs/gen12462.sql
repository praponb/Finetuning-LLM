select
    p.price,
    p.brand,
    oi.order_id,
    oi.quantity,
    oi.product_id
from products p
left join order_items oi on oi.product_id = p.id
where oi.product_id in (8, 15) or p.stock_qty is not null
order by quantity desc;
