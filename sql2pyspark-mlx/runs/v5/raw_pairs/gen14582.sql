select
    p.stock_qty,
    p.launch_date,
    p.price,
    oi.product_id,
    oi.order_id
from products p
left join order_items oi on oi.product_id = p.id
order by p.price desc
