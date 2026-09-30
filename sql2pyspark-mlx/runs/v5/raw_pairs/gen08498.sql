select
    p.id,
    p.category,
    count(oi.id) as order_item_count,
    min(oi.quantity) as min_quantity,
    avg(oi.unit_price) as avg_unit_price
from products p
left outer join order_items oi on p.id = oi.product_id
group by p.id, p.category;
