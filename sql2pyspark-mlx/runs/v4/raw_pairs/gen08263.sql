select
    p.cost,
    p.launch_date,
    p.brand,
    oi.product_id,
    oi.unit_price + oi.quantity as line_total
from products p
left outer join order_items oi on oi.product_id = p.id
where oi.quantity = 4
order by brand desc
