select
    oi.unit_price,
    oi.quantity,
    o.customer_id,
    o.discount,
    p.stock_qty
from order_items oi
right join orders o on oi.order_id = o.id
inner join products p on oi.product_id = p.id
order by 1 desc
